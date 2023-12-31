use stackable_operator::builder::resources::ResourceRequirementsBuilder;

use crate::odoo_controller::DOCKER_IMAGE_BASE_NAME;
use crate::controller_commons::{CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME};
use crate::product_logging::{
    extend_config_map_with_log_config, resolve_vector_aggregator_address,
};
use crate::utils::{env_var_from_secret, get_job_state, JobState};
use crate::{controller_commons, rbac};

use snafu::{OptionExt, ResultExt, Snafu};
use sovrin_cloud_crd::{
    odoodb::{
        OdooDB, OdooDBStatus, OdooDBStatusCondition, OdooDbConfig, Container,
        AIRFLOW_DB_CONTROLLER_NAME,
    },
    AIRFLOW_UID, LOG_CONFIG_DIR, STACKABLE_LOG_DIR,
};

use stackable_operator::{
    builder::{ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodSecurityContextBuilder},
    commons::product_image_selection::ResolvedProductImage,
    k8s_openapi::api::{
        batch::v1::{Job, JobSpec},
        core::v1::{ConfigMap, EnvVar, PodSpec, PodTemplateSpec, Secret},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        ResourceExt,
    },
    logging::controller::ReconcilerError,
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
};
use std::{sync::Arc, time::Duration};
use strum::{EnumDiscriminants, IntoStaticStr};

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("failed to apply Job for {}", odoo_db))]
    ApplyJob {
        source: stackable_operator::error::Error,
        odoo_db: ObjectRef<OdooDB>,
    },
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("database state is 'initializing' but failed to find job {}", init_job))]
    GetInitializationJob {
        source: stackable_operator::error::Error,
        init_job: ObjectRef<Job>,
    },
    #[snafu(display("Failed to check whether the secret ({}) exists", secret))]
    SecretCheck {
        source: stackable_operator::error::Error,
        secret: ObjectRef<Secret>,
    },
    #[snafu(display("failed to patch service account: {source}"))]
    ApplyServiceAccount {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build ConfigMap [{name}]"))]
    BuildConfig {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch ConfigMap [{name}]"))]
    ApplyConfigMap {
        name: String,
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve and merge config"))]
    FailedToResolveConfig {
        source: sovrin_cloud_crd::odoodb::Error,
    },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to resolve the Vector aggregator address"))]
    ResolveVectorAggregatorAddress {
        source: crate::product_logging::Error,
    },
    #[snafu(display("failed to add the logging configuration to the ConfigMap [{cm_name}]"))]
    InvalidLoggingConfig {
        source: crate::product_logging::Error,
        cm_name: String,
    },
}
type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_odoo_db(odoo_db: Arc<OdooDB>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;
    let namespace = odoo_db.namespace().context(ObjectHasNoNamespaceSnafu)?;
    let resolved_product_image: ResolvedProductImage =
        odoo_db.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let (rbac_sa, rbac_rolebinding) = rbac::build_rbac_resources(odoo_db.as_ref(), "odoo");
    client
        .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &rbac_sa, &rbac_sa)
        .await
        .with_context(|_| ApplyServiceAccountSnafu {
            name: rbac_sa.name_unchecked(),
        })?;
    client
        .apply_patch(
            AIRFLOW_DB_CONTROLLER_NAME,
            &rbac_rolebinding,
            &rbac_rolebinding,
        )
        .await
        .with_context(|_| ApplyRoleBindingSnafu {
            name: rbac_rolebinding.name_unchecked(),
        })?;
    if let Some(ref s) = odoo_db.status {
        match s.condition {
            OdooDBStatusCondition::Pending => {
                // This is easier to use than `get_opt` and having an Error variant for "Secret does not exist"
                let _secret = client
                    .get::<Secret>(&odoo_db.spec.credentials_secret, &namespace)
                    .await
                    .context(SecretCheckSnafu {
                        secret: ObjectRef::<Secret>::new(&odoo_db.spec.credentials_secret)
                            .within(&namespace),
                    })?;

                let vector_aggregator_address = resolve_vector_aggregator_address(
                    client,
                    odoo_db.as_ref(),
                    odoo_db.spec.vector_aggregator_config_map_name.as_deref(),
                )
                    .await
                    .context(ResolveVectorAggregatorAddressSnafu)?;

                let config = odoo_db
                    .merged_config()
                    .context(FailedToResolveConfigSnafu)?;

                let config_map = build_config_map(
                    &odoo_db,
                    &config.logging,
                    vector_aggregator_address.as_deref(),
                )?;
                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &config_map, &config_map)
                    .await
                    .context(ApplyConfigMapSnafu {
                        name: config_map.name_any(),
                    })?;

                let job = build_init_job(
                    &odoo_db,
                    &resolved_product_image,
                    &rbac_sa.name_unchecked(),
                    &config,
                    &config_map.name_unchecked(),
                )?;
                client
                    .apply_patch(AIRFLOW_DB_CONTROLLER_NAME, &job, &job)
                    .await
                    .context(ApplyJobSnafu {
                        odoo_db: ObjectRef::from_obj(&*odoo_db),
                    })?;
                // The job is started, update status to reflect new state
                client
                    .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*odoo_db, &s.initializing())
                    .await
                    .context(ApplyStatusSnafu)?;
            }
            OdooDBStatusCondition::Initializing => {
                // In here, check the associated job that is running.
                // If it is still running, do nothing. If it completed, set status to ready, if it failed, set status to failed.
                let job_name = odoo_db.job_name();
                let job = client.get::<Job>(&job_name, &namespace).await.context(
                    GetInitializationJobSnafu {
                        init_job: ObjectRef::<Job>::new(&job_name).within(&namespace),
                    },
                )?;

                let new_status = match get_job_state(&job) {
                    JobState::Complete => Some(s.ready()),
                    JobState::Failed => Some(s.failed()),
                    JobState::InProgress => None,
                };

                if let Some(ns) = new_status {
                    client
                        .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*odoo_db, &ns)
                        .await
                        .context(ApplyStatusSnafu)?;
                }
            }
            OdooDBStatusCondition::Ready => (),
            OdooDBStatusCondition::Failed => (),
        }
    } else {
        // Status is none => initialize the status object as "Provisioned"
        let new_status = OdooDBStatus::new();
        client
            .apply_patch_status(AIRFLOW_DB_CONTROLLER_NAME, &*odoo_db, &new_status)
            .await
            .context(ApplyStatusSnafu)?;
    }

    Ok(Action::await_change())
}

fn build_init_job(
    odoo_db: &OdooDB,
    resolved_product_image: &ResolvedProductImage,
    sa_name: &str,
    config: &OdooDbConfig,
    config_map_name: &str,
) -> Result<Job> {
    let commands = vec![
        String::from("odoo db init"),
        String::from("odoo db upgrade"),
        String::from(
            "odoo users create \
                    --username \"$ADMIN_USERNAME\" \
                    --firstname \"$ADMIN_FIRSTNAME\" \
                    --lastname \"$ADMIN_LASTNAME\" \
                    --email \"$ADMIN_EMAIL\" \
                    --password \"$ADMIN_PASSWORD\" \
                    --role \"Admin\"",
        ),
        product_logging::framework::shutdown_vector_command(STACKABLE_LOG_DIR),
    ];

    let secret = &odoo_db.spec.credentials_secret;

    let env = vec![
        env_var_from_secret(
            "AIRFLOW__WEBSERVER__SECRET_KEY",
            secret,
            "connections.secretKey",
        ),
        env_var_from_secret(
            "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
            secret,
            "connections.sqlalchemyDatabaseUri",
        ),
        env_var_from_secret(
            "AIRFLOW__CELERY__RESULT_BACKEND",
            secret,
            "connections.celeryResultBackend",
        ),
        env_var_from_secret("ADMIN_USERNAME", secret, "adminUser.username"),
        env_var_from_secret("ADMIN_FIRSTNAME", secret, "adminUser.firstname"),
        env_var_from_secret("ADMIN_LASTNAME", secret, "adminUser.lastname"),
        env_var_from_secret("ADMIN_EMAIL", secret, "adminUser.email"),
        env_var_from_secret("ADMIN_PASSWORD", secret, "adminUser.password"),
        EnvVar {
            name: "PYTHONPATH".into(),
            value: Some(LOG_CONFIG_DIR.into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__LOGGING__LOGGING_CONFIG_CLASS".into(),
            value: Some("log_config.LOGGING_CONFIG".into()),
            ..Default::default()
        },
    ];

    let mut containers = Vec::new();

    let mut cb = ContainerBuilder::new(&Container::OdooInitDb.to_string())
        .context(InvalidContainerNameSnafu)?;

    cb.image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")])
        .add_env_vars(env)
        .add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR)
        .add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR)
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("400m")
                .with_memory_request("512Mi")
                .with_memory_limit("512Mi")
                .build(),
        );

    let volumes = controller_commons::create_volumes(
        config_map_name,
        config.logging.containers.get(&Container::OdooInitDb),
    );

    containers.push(cb.build());

    if config.logging.enable_vector_agent {
        containers.push(product_logging::framework::vector_container(
            resolved_product_image,
            CONFIG_VOLUME_NAME,
            LOG_VOLUME_NAME,
            config.logging.containers.get(&Container::Vector),
            ResourceRequirementsBuilder::new()
                .with_cpu_request("250m")
                .with_cpu_limit("500m")
                .with_memory_request("128Mi")
                .with_memory_limit("128Mi")
                .build(),
        ));
    }

    let pod = PodTemplateSpec {
        metadata: Some(
            ObjectMetaBuilder::new()
                .name(format!("{}-init", odoo_db.name_unchecked()))
                .build(),
        ),
        spec: Some(PodSpec {
            containers,
            restart_policy: Some("Never".to_string()),
            service_account: Some(sa_name.to_string()),
            image_pull_secrets: resolved_product_image.pull_secrets.clone(),
            security_context: Some(
                PodSecurityContextBuilder::new()
                    .run_as_user(AIRFLOW_UID)
                    .run_as_group(0)
                    .build(),
            ),
            volumes: Some(volumes),
            ..Default::default()
        }),
    };

    let job = Job {
        metadata: ObjectMetaBuilder::new()
            .name(odoo_db.name_unchecked())
            .namespace_opt(odoo_db.namespace())
            .ownerreference_from_resource(odoo_db, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
        spec: Some(JobSpec {
            template: pod,
            ..Default::default()
        }),
        status: None,
    };

    Ok(job)
}

fn build_config_map(
    odoo_db: &OdooDB,
    logging: &Logging<Container>,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap> {
    let mut cm_builder = ConfigMapBuilder::new();

    let cm_name = format!("{cluster}-init-db", cluster = odoo_db.name_unchecked());

    cm_builder.metadata(
        ObjectMetaBuilder::new()
            .name(&cm_name)
            .namespace_opt(odoo_db.namespace())
            .ownerreference_from_resource(odoo_db, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .build(),
    );

    extend_config_map_with_log_config(
        &RoleGroupRef {
            cluster: ObjectRef::from_obj(odoo_db),
            role: String::new(),
            role_group: String::new(),
        },
        vector_aggregator_address,
        logging,
        &Container::OdooInitDb,
        &Container::Vector,
        &mut cm_builder,
    )
        .context(InvalidLoggingConfigSnafu {
            cm_name: cm_name.to_owned(),
        })?;

    cm_builder
        .build()
        .context(BuildConfigSnafu { name: cm_name })
}

pub fn error_policy(_obj: Arc<OdooDB>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}