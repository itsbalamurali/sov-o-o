//! Ensures that `Pod`s are configured and running for each [`OdooCluster`]
use stackable_operator::builder::resources::ResourceRequirementsBuilder;
use stackable_operator::k8s_openapi::DeepMerge;

use crate::config::{self, PYTHON_IMPORTS};
use crate::controller_commons::{
    self, CONFIG_VOLUME_NAME, LOG_CONFIG_VOLUME_NAME, LOG_VOLUME_NAME,
};
use crate::product_logging::{
    extend_config_map_with_log_config, resolve_vector_aggregator_address,
};
use crate::utils::env_var_from_secret;

use snafu::{OptionExt, ResultExt, Snafu};
use sovrin_cloud_crd::odoodb::OdooDBStatus;
use sovrin_cloud_crd::{
    odoodb::{OdooDB, OdooDBStatusCondition},
    build_recommended_labels, OdooCluster, OdooConfig, OdooConfigFragment,
    OdooConfigOptions, OdooRole, Container, AIRFLOW_CONFIG_FILENAME, APP_NAME, CONFIG_PATH,
    LOG_CONFIG_DIR, OPERATOR_NAME, STACKABLE_LOG_DIR,
};
use sovrin_cloud_crd::{
    OdooClusterStatus, AIRFLOW_UID, GIT_CONTENT, GIT_LINK, GIT_ROOT, GIT_SYNC_DIR, GIT_SYNC_NAME,
};
use stackable_operator::builder::VolumeBuilder;
use stackable_operator::k8s_openapi::api::core::v1::EmptyDirVolumeSource;
use stackable_operator::{
    builder::{
        ConfigMapBuilder, ContainerBuilder, ObjectMetaBuilder, PodBuilder,
        PodSecurityContextBuilder,
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::{AuthenticationClass, AuthenticationClassProvider},
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, EnvVar, Probe, Service, ServicePort, ServiceSpec, TCPSocketAction,
            },
        },
        apimachinery::pkg::{apis::meta::v1::LabelSelector, util::intstr::IntOrString},
    },
    kube::{
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    labels::{role_group_selector_labels, role_selector_labels},
    logging::controller::ReconcilerError,
    product_config::{
        flask_app_config_writer, flask_app_config_writer::FlaskAppConfigWriterError,
        types::PropertyNameKind, ProductConfigManager,
    },
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{self, spec::Logging},
    role_utils::RoleGroupRef,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder, ClusterCondition, ClusterConditionSet,
        ClusterConditionStatus, ClusterConditionType, ConditionBuilder,
    },
};
use std::{
    collections::{BTreeMap, HashMap},
    str::FromStr,
    sync::Arc,
    time::Duration,
};
use strum::{EnumDiscriminants, IntoEnumIterator, IntoStaticStr};

pub const AIRFLOW_CONTROLLER_NAME: &str = "odoocluster";
pub const DOCKER_IMAGE_BASE_NAME: &str = "odoo";

const METRICS_PORT_NAME: &str = "metrics";
const METRICS_PORT: i32 = 9102;

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
pub enum Error {
    #[snafu(display("object has no namespace"))]
    ObjectHasNoNamespace,
    #[snafu(display("object defines no odoo config role"))]
    NoOdooRole,
    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to apply Service for {rolegroup}"))]
    ApplyRoleGroupService {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<OdooCluster>,
    },
    #[snafu(display("failed to apply ConfigMap for {rolegroup}"))]
    ApplyRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<OdooCluster>,
    },
    #[snafu(display("failed to apply StatefulSet for {rolegroup}"))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<OdooCluster>,
    },
    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("object is missing metadata to build owner reference"))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("Failed to transform configs"))]
    ProductConfigTransform {
        source: stackable_operator::product_config_utils::ConfigError,
    },
    #[snafu(display("failed to apply Odoo DB"))]
    CreateOdooDBObject {
        source: sovrin_cloud_crd::odoodb::Error,
    },
    #[snafu(display("failed to apply Odoo DB"))]
    ApplyOdooDB {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve Odoo DB"))]
    OdooDBRetrieval {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch service account"))]
    ApplyServiceAccount {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to patch role binding: {source}"))]
    ApplyRoleBinding {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to build RBAC objects"))]
    BuildRBACObjects {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to retrieve AuthenticationClass {authentication_class}"))]
    AuthenticationClassRetrieval {
        source: stackable_operator::error::Error,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display(
    "Odoo doesn't support the AuthenticationClass provider
    {authentication_class_provider} from AuthenticationClass {authentication_class}"
    ))]
    AuthenticationClassProviderNotSupported {
        authentication_class_provider: String,
        authentication_class: ObjectRef<AuthenticationClass>,
    },
    #[snafu(display("failed to build config file for {rolegroup}"))]
    BuildRoleGroupConfigFile {
        source: FlaskAppConfigWriterError,
        rolegroup: RoleGroupRef<OdooCluster>,
    },
    #[snafu(display("failed to build ConfigMap for {rolegroup}"))]
    BuildRoleGroupConfig {
        source: stackable_operator::error::Error,
        rolegroup: RoleGroupRef<OdooCluster>,
    },
    #[snafu(display("Odoo db {odoo_db} initialization failed, not starting odoo"))]
    OdooDBFailed { odoo_db: ObjectRef<OdooDB> },
    #[snafu(display("failed to resolve and merge config for role and role group"))]
    FailedToResolveConfig {
        source: sovrin_cloud_crd::Error,
    },
    #[snafu(display("could not parse Odoo role [{role}]"))]
    UnidentifiedOdooRole {
        source: strum::ParseError,
        role: String,
    },
    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::error::Error,
    },
    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphanedResources {
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
    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::error::Error,
    },
}

type Result<T, E = Error> = std::result::Result<T, E>;

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
}

pub async fn reconcile_odoo(odoo: Arc<OdooCluster>, ctx: Arc<Ctx>) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let client = &ctx.client;
    let resolved_product_image: ResolvedProductImage =
        odoo.spec.image.resolve(DOCKER_IMAGE_BASE_NAME);

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&odoo.spec.cluster_operation);

    if wait_for_db_and_update_status(
        client,
        &odoo,
        &resolved_product_image,
        &cluster_operation_cond_builder,
    )
        .await?
    {
        return Ok(Action::await_change());
    }

    let mut roles = HashMap::new();

    for role in OdooRole::iter() {
        if let Some(resolved_role) = odoo.get_role(&role).clone() {
            roles.insert(
                role.to_string(),
                (
                    vec![
                        PropertyNameKind::Env,
                        PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.into()),
                    ],
                    resolved_role,
                ),
            );
        }
    }

    let role_config = transform_all_roles_to_config::<OdooConfigFragment>(&odoo, roles);
    let validated_role_config = validate_all_roles_and_groups_config(
        &resolved_product_image.product_version,
        &role_config.context(ProductConfigTransformSnafu)?,
        &ctx.product_config,
        false,
        false,
    )
        .context(InvalidProductConfigSnafu)?;

    let vector_aggregator_address = resolve_vector_aggregator_address(
        client,
        odoo.as_ref(),
        odoo
            .spec
            .cluster_config
            .vector_aggregator_config_map_name
            .as_deref(),
    )
        .await
        .context(ResolveVectorAggregatorAddressSnafu)?;

    let authentication_class = match &odoo.spec.cluster_config.authentication_config {
        Some(authentication_config) => match &authentication_config.authentication_class {
            Some(authentication_class) => Some(
                AuthenticationClass::resolve(client, authentication_class)
                    .await
                    .context(AuthenticationClassRetrievalSnafu {
                        authentication_class: ObjectRef::<AuthenticationClass>::new(
                            authentication_class,
                        ),
                    })?,
            ),
            None => None,
        },
        None => None,
    };

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        AIRFLOW_CONTROLLER_NAME,
        &odoo.object_ref(&()),
        ClusterResourceApplyStrategy::from(&odoo.spec.cluster_operation),
    )
        .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        odoo.as_ref(),
        APP_NAME,
        cluster_resources.get_required_labels(),
    )
        .context(BuildRBACObjectsSnafu)?;

    let rbac_sa = cluster_resources
        .add(client, rbac_sa)
        .await
        .context(ApplyServiceAccountSnafu)?;
    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    for (role_name, role_config) in validated_role_config.iter() {
        // some roles will only run "internally" and do not need to be created as services
        if let Some(resolved_port) = role_port(role_name) {
            let role_service =
                build_role_service(&odoo, &resolved_product_image, role_name, resolved_port)?;
            cluster_resources
                .add(client, role_service)
                .await
                .context(ApplyRoleServiceSnafu)?;
        }

        for (rolegroup_name, rolegroup_config) in role_config.iter() {
            let rolegroup = RoleGroupRef {
                cluster: ObjectRef::from_obj(&*odoo),
                role: role_name.into(),
                role_group: rolegroup_name.into(),
            };

            let odoo_role =
                OdooRole::from_str(role_name).context(UnidentifiedOdooRoleSnafu {
                    role: role_name.to_string(),
                })?;

            let config = odoo
                .merged_config(&odoo_role, &rolegroup)
                .context(FailedToResolveConfigSnafu)?;

            let rg_service =
                build_rolegroup_service(&odoo, &resolved_product_image, &rolegroup)?;
            cluster_resources.add(client, rg_service).await.context(
                ApplyRoleGroupServiceSnafu {
                    rolegroup: rolegroup.clone(),
                },
            )?;

            let rg_configmap = build_rolegroup_config_map(
                &odoo,
                &resolved_product_image,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
                &config.logging,
                vector_aggregator_address.as_deref(),
            )?;
            cluster_resources
                .add(client, rg_configmap)
                .await
                .with_context(|_| ApplyRoleGroupConfigSnafu {
                    rolegroup: rolegroup.clone(),
                })?;

            let rg_statefulset = build_server_rolegroup_statefulset(
                &odoo,
                &resolved_product_image,
                &odoo_role,
                &rolegroup,
                rolegroup_config,
                authentication_class.as_ref(),
                &rbac_sa.name_unchecked(),
                &config,
            )?;

            ss_cond_builder.add(
                cluster_resources
                    .add(client, rg_statefulset)
                    .await
                    .context(ApplyRoleGroupStatefulSetSnafu {
                        rolegroup: rolegroup.clone(),
                    })?,
            );
        }
    }

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphanedResourcesSnafu)?;

    let status = OdooClusterStatus {
        conditions: compute_conditions(
            odoo.as_ref(),
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

    client
        .apply_patch_status(OPERATOR_NAME, &*odoo, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
fn build_role_service(
    odoo: &OdooCluster,
    resolved_product_image: &ResolvedProductImage,
    role_name: &str,
    port: u16,
) -> Result<Service> {
    let role_svc_name = format!(
        "{}-{}",
        odoo
            .metadata
            .name
            .as_ref()
            .unwrap_or(&APP_NAME.to_string()),
        role_name
    );
    let ports = role_ports(port);

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(odoo)
            .name(&role_svc_name)
            .ownerreference_from_resource(odoo, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                odoo,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                role_name,
                "global",
            ))
            .build(),
        spec: Some(ServiceSpec {
            type_: Some(
                odoo
                    .spec
                    .cluster_config
                    .listener_class
                    .k8s_service_type(),
            ),
            ports: Some(ports),
            selector: Some(role_selector_labels(odoo, APP_NAME, role_name)),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

fn role_ports(port: u16) -> Vec<ServicePort> {
    vec![ServicePort {
        name: Some(APP_NAME.to_string()),
        port: port.into(),
        protocol: Some("TCP".to_string()),
        ..ServicePort::default()
    }]
}

fn role_port(role_name: &str) -> Option<u16> {
    OdooRole::from_str(role_name).unwrap().get_http_port()
}

/// The rolegroup [`ConfigMap`] configures the rolegroup based on the configuration given by the administrator
fn build_rolegroup_config_map(
    odoo: &OdooCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<OdooCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
    logging: &Logging<Container>,
    vector_aggregator_address: Option<&str>,
) -> Result<ConfigMap, Error> {
    let mut config = rolegroup_config
        .get(&PropertyNameKind::File(AIRFLOW_CONFIG_FILENAME.to_string()))
        .cloned()
        .unwrap_or_default();

    config::add_odoo_config(
        &mut config,
        odoo.spec.cluster_config.authentication_config.as_ref(),
        authentication_class,
    );

    let mut config_file = Vec::new();
    flask_app_config_writer::write::<OdooConfigOptions, _, _>(
        &mut config_file,
        config.iter(),
        PYTHON_IMPORTS,
    )
        .with_context(|_| BuildRoleGroupConfigFileSnafu {
            rolegroup: rolegroup.clone(),
        })?;

    let mut cm_builder = ConfigMapBuilder::new();

    cm_builder
        .metadata(
            ObjectMetaBuilder::new()
                .name_and_namespace(odoo)
                .name(rolegroup.object_name())
                .ownerreference_from_resource(odoo, None, Some(true))
                .context(ObjectMissingMetadataForOwnerRefSnafu)?
                .with_recommended_labels(build_recommended_labels(
                    odoo,
                    AIRFLOW_CONTROLLER_NAME,
                    &resolved_product_image.app_version_label,
                    &rolegroup.role,
                    &rolegroup.role_group,
                ))
                .build(),
        )
        .add_data(
            AIRFLOW_CONFIG_FILENAME,
            String::from_utf8(config_file).unwrap(),
        );

    extend_config_map_with_log_config(
        rolegroup,
        vector_aggregator_address,
        logging,
        &Container::Odoo,
        &Container::Vector,
        &mut cm_builder,
    )
        .context(InvalidLoggingConfigSnafu {
            cm_name: rolegroup.object_name(),
        })?;

    cm_builder
        .build()
        .with_context(|_| BuildRoleGroupConfigSnafu {
            rolegroup: rolegroup.clone(),
        })
}

/// The rolegroup [`Service`] is a headless service that allows direct access to the instances of a certain rolegroup
///
/// This is mostly useful for internal communication between peers, or for clients that perform client-side load balancing.
fn build_rolegroup_service(
    odoo: &OdooCluster,
    resolved_product_image: &ResolvedProductImage,
    rolegroup: &RoleGroupRef<OdooCluster>,
) -> Result<Service> {
    let mut ports = vec![ServicePort {
        name: Some(METRICS_PORT_NAME.into()),
        port: METRICS_PORT,
        protocol: Some("TCP".to_string()),
        ..Default::default()
    }];

    if let Some(http_port) = role_port(&rolegroup.role) {
        ports.append(&mut role_ports(http_port));
    }

    Ok(Service {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(odoo)
            .name(&rolegroup.object_name())
            .ownerreference_from_resource(odoo, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                odoo,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup.role,
                &rolegroup.role_group,
            ))
            .with_label("prometheus.io/scrape", "true")
            .build(),
        spec: Some(ServiceSpec {
            // Internal communication does not need to be exposed
            type_: Some("ClusterIP".to_string()),
            cluster_ip: Some("None".to_string()),
            ports: Some(ports),
            selector: Some(role_group_selector_labels(
                odoo,
                APP_NAME,
                &rolegroup.role,
                &rolegroup.role_group,
            )),
            publish_not_ready_addresses: Some(true),
            ..ServiceSpec::default()
        }),
        status: None,
    })
}

/// The rolegroup [`StatefulSet`] runs the rolegroup, as configured by the administrator.
///
/// The [`Pod`](`stackable_operator::k8s_openapi::api::core::v1::Pod`)s are accessible through the corresponding [`Service`] (from [`build_rolegroup_service`]).
#[allow(clippy::too_many_arguments)]
fn build_server_rolegroup_statefulset(
    odoo: &OdooCluster,
    resolved_product_image: &ResolvedProductImage,
    odoo_role: &OdooRole,
    rolegroup_ref: &RoleGroupRef<OdooCluster>,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
    authentication_class: Option<&AuthenticationClass>,
    sa_name: &str,
    config: &OdooConfig,
) -> Result<StatefulSet> {
    let role = odoo
        .get_role(odoo_role)
        .as_ref()
        .context(NoOdooRoleSnafu)?;

    let rolegroup = role.role_groups.get(&rolegroup_ref.role_group);

    let commands = odoo_role.get_commands();

    let mut pb = PodBuilder::new();
    pb.metadata_builder(|m| {
        m.with_recommended_labels(build_recommended_labels(
            odoo,
            AIRFLOW_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &rolegroup_ref.role,
            &rolegroup_ref.role_group,
        ))
    })
        .image_pull_secrets_from_product_image(resolved_product_image)
        .affinity(&config.affinity)
        .service_account_name(sa_name)
        .security_context(
            PodSecurityContextBuilder::new()
                .run_as_user(AIRFLOW_UID)
                .run_as_group(0)
                .fs_group(1000) // Needed for secret-operator
                .build(),
        );

    let mut odoo_container = ContainerBuilder::new(&Container::Odoo.to_string())
        .context(InvalidContainerNameSnafu)?;

    if let Some(authentication_class) = authentication_class {
        add_authentication_volumes_and_volume_mounts(
            authentication_class,
            &mut odoo_container,
            &mut pb,
        )?;
    }

    odoo_container
        .image_from_product_image(resolved_product_image)
        .resources(config.resources.clone().into())
        .command(vec!["/bin/bash".to_string()])
        .args(vec![String::from("-c"), commands.join("; ")]);

    // environment variables
    let env_config = rolegroup_config
        .get(&PropertyNameKind::Env)
        .iter()
        .flat_map(|env_vars| env_vars.iter())
        .map(|(k, v)| EnvVar {
            name: k.clone(),
            value: Some(v.clone()),
            ..EnvVar::default()
        })
        .collect::<Vec<_>>();

    // mapped environment variables
    let env_mapped = build_mapped_envs(odoo, rolegroup_config);

    odoo_container.add_env_vars(env_config);
    odoo_container.add_env_vars(env_mapped);
    odoo_container.add_env_vars(build_static_envs());

    let volume_mounts = odoo.volume_mounts();
    odoo_container.add_volume_mounts(volume_mounts);
    odoo_container.add_volume_mount(CONFIG_VOLUME_NAME, CONFIG_PATH);
    odoo_container.add_volume_mount(LOG_CONFIG_VOLUME_NAME, LOG_CONFIG_DIR);
    odoo_container.add_volume_mount(LOG_VOLUME_NAME, STACKABLE_LOG_DIR);

    if let Some(resolved_port) = odoo_role.get_http_port() {
        let probe = Probe {
            tcp_socket: Some(TCPSocketAction {
                port: IntOrString::Int(resolved_port.into()),
                ..TCPSocketAction::default()
            }),
            initial_delay_seconds: Some(20),
            period_seconds: Some(5),
            ..Probe::default()
        };
        odoo_container.readiness_probe(probe.clone());
        odoo_container.liveness_probe(probe);
        odoo_container.add_container_port("http", resolved_port.into());
    }

    pb.add_container(odoo_container.build());

    let metrics_container = ContainerBuilder::new("metrics")
        .context(InvalidContainerNameSnafu)?
        .image_from_product_image(resolved_product_image)
        .command(vec!["/bin/bash".to_string(), "-c".to_string()])
        .args(vec!["/stackable/statsd_exporter".to_string()])
        .add_container_port(METRICS_PORT_NAME, METRICS_PORT)
        .resources(
            ResourceRequirementsBuilder::new()
                .with_cpu_request("100m")
                .with_cpu_limit("200m")
                .with_memory_request("64Mi")
                .with_memory_limit("64Mi")
                .build(),
        )
        .build();
    pb.add_container(metrics_container);

    pb.add_volumes(odoo.volumes());
    pb.add_volumes(controller_commons::create_volumes(
        &rolegroup_ref.object_name(),
        config.logging.containers.get(&Container::Odoo),
    ));

    if let Some(gitsync) = odoo.git_sync() {
        let gitsync_container = ContainerBuilder::new(&format!("{}-{}", GIT_SYNC_NAME, 1))
            .context(InvalidContainerNameSnafu)?
            .add_env_vars(build_gitsync_envs(rolegroup_config))
            .image_from_product_image(resolved_product_image)
            .command(vec!["/bin/bash".to_string(), "-c".to_string()])
            .args(vec![gitsync.get_args().join(" ")])
            .add_volume_mount(GIT_CONTENT, GIT_ROOT)
            .resources(
                ResourceRequirementsBuilder::new()
                    .with_cpu_request("100m")
                    .with_cpu_limit("200m")
                    .with_memory_request("64Mi")
                    .with_memory_limit("64Mi")
                    .build(),
            )
            .build();

        pb.add_volume(
            VolumeBuilder::new(GIT_CONTENT)
                .empty_dir(EmptyDirVolumeSource::default())
                .build(),
        );
        pb.add_container(gitsync_container);
    }

    if config.logging.enable_vector_agent {
        pb.add_container(product_logging::framework::vector_container(
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

    let mut pod_template = pb.build_template();
    pod_template.merge_from(role.config.pod_overrides.clone());
    if let Some(rolegroup) = rolegroup {
        pod_template.merge_from(rolegroup.config.pod_overrides.clone());
    }

    Ok(StatefulSet {
        metadata: ObjectMetaBuilder::new()
            .name_and_namespace(odoo)
            .name(&rolegroup_ref.object_name())
            .ownerreference_from_resource(odoo, None, Some(true))
            .context(ObjectMissingMetadataForOwnerRefSnafu)?
            .with_recommended_labels(build_recommended_labels(
                odoo,
                AIRFLOW_CONTROLLER_NAME,
                &resolved_product_image.app_version_label,
                &rolegroup_ref.role,
                &rolegroup_ref.role_group,
            ))
            .with_label("restarter.stackable.tech/enabled", "true")
            .build(),
        spec: Some(StatefulSetSpec {
            pod_management_policy: Some("Parallel".to_string()),
            replicas: rolegroup.and_then(|rg| rg.replicas).map(i32::from),
            selector: LabelSelector {
                match_labels: Some(role_group_selector_labels(
                    odoo,
                    APP_NAME,
                    &rolegroup_ref.role,
                    &rolegroup_ref.role_group,
                )),
                ..LabelSelector::default()
            },
            service_name: rolegroup_ref.object_name(),
            template: pod_template,
            ..StatefulSetSpec::default()
        }),
        status: None,
    })
}

/// This builds a collection of environment variables some require some minimal mapping,
/// such as executor type, contents of the secret etc.
fn build_mapped_envs(
    odoo: &OdooCluster,
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let secret_prop = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(OdooConfig::CREDENTIALS_SECRET_PROPERTY));

    let mut env = secret_prop
        .map(|secret| {
            vec![
                // The secret key is used to run the webserver flask app and also used to authorize
                // requests to Celery workers when logs are retrieved.
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
                env_var_from_secret(
                    "AIRFLOW__CELERY__BROKER_URL",
                    secret,
                    "connections.celeryBrokerUrl",
                ),
            ]
        })
        .unwrap_or_default();

    if let Some(git_sync) = &odoo.git_sync() {
        if let Some(dags_folder) = &git_sync.git_folder {
            env.push(EnvVar {
                name: "AIRFLOW__CORE__DAGS_FOLDER".into(),
                value: Some(format!("{GIT_SYNC_DIR}/{GIT_LINK}/{dags_folder}")),
                ..Default::default()
            })
        }
    }

    if let Some(true) = odoo.spec.cluster_config.load_examples {
        env.push(EnvVar {
            name: "AIRFLOW__CORE__LOAD_EXAMPLES".into(),
            value: Some("True".into()),
            ..Default::default()
        })
    } else {
        env.push(EnvVar {
            name: "AIRFLOW__CORE__LOAD_EXAMPLES".into(),
            value: Some("False".into()),
            ..Default::default()
        })
    }

    if let Some(true) = odoo.spec.cluster_config.expose_config {
        env.push(EnvVar {
            name: "AIRFLOW__WEBSERVER__EXPOSE_CONFIG".into(),
            value: Some("True".into()),
            ..Default::default()
        })
    }

    let executor = odoo.spec.cluster_config.executor.clone();

    env.push(EnvVar {
        name: "AIRFLOW__CORE__EXECUTOR".into(),
        value: executor,
        ..Default::default()
    });

    env
}

fn build_gitsync_envs(
    rolegroup_config: &HashMap<PropertyNameKind, BTreeMap<String, String>>,
) -> Vec<EnvVar> {
    let mut env = vec![];
    if let Some(git_secret) = rolegroup_config
        .get(&PropertyNameKind::Env)
        .and_then(|vars| vars.get(OdooConfig::GIT_CREDENTIALS_SECRET_PROPERTY))
    {
        env.push(env_var_from_secret("GIT_SYNC_USERNAME", git_secret, "user"));
        env.push(env_var_from_secret(
            "GIT_SYNC_PASSWORD",
            git_secret,
            "password",
        ));
    }

    env
}

fn build_static_envs() -> Vec<EnvVar> {
    [
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
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_ON".into(),
            value: Some("True".into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_HOST".into(),
            value: Some("0.0.0.0".into()),
            ..Default::default()
        },
        EnvVar {
            name: "AIRFLOW__METRICS__STATSD_PORT".into(),
            value: Some("9125".into()),
            ..Default::default()
        },
        // Authentication for the API is handled separately to the Web Authentication.
        // Basic authentication is used by the integration tests.
        // The default is to deny all requests to the API.
        EnvVar {
            name: "AIRFLOW__API__AUTH_BACKEND".into(),
            value: Some("odoo.api.auth.backend.basic_auth".into()),
            ..Default::default()
        },
    ]
        .into()
}

pub fn error_policy(_obj: Arc<OdooCluster>, _error: &Error, _ctx: Arc<Ctx>) -> Action {
    Action::requeue(Duration::from_secs(5))
}

fn add_authentication_volumes_and_volume_mounts(
    authentication_class: &AuthenticationClass,
    cb: &mut ContainerBuilder,
    pb: &mut PodBuilder,
) -> Result<()> {
    match &authentication_class.spec.provider {
        AuthenticationClassProvider::Ldap(ldap) => {
            ldap.add_volumes_and_mounts(pb, vec![cb]);
            Ok(())
        }
        _ => AuthenticationClassProviderNotSupportedSnafu {
            authentication_class_provider: authentication_class.spec.provider.to_string(),
            authentication_class: ObjectRef::<AuthenticationClass>::new(
                &authentication_class.name_unchecked(),
            ),
        }
            .fail(),
    }
}

/// Return true if the controller should wait for the DB to be set up.
///
/// As a side-effect, the Odoo cluster status is updated as long as the controller waits
/// for the DB to come up.
///
/// Having the DB set up by a Job managed by a different controller has it's own
/// set of problems as described here: <https://github.com/stackabletech/superset-operator/issues/351>.
/// The Superset operator uses the same pattern as implemented here for setting up the DB.
///
/// When the ticket above is implemented, this function will most likely be removed completely.
async fn wait_for_db_and_update_status(
    client: &stackable_operator::client::Client,
    odoo: &OdooCluster,
    resolved_product_image: &ResolvedProductImage,
    cluster_operation_condition_builder: &ClusterOperationsConditionBuilder<'_>,
) -> Result<bool> {
    // ensure admin user has been set up on the odoo database
    let odoo_db = OdooDB::for_odoo(odoo, resolved_product_image)
        .context(CreateOdooDBObjectSnafu)?;
    client
        .apply_patch(AIRFLOW_CONTROLLER_NAME, &odoo_db, &odoo_db)
        .await
        .context(ApplyOdooDBSnafu)?;

    let odoo_db = client
        .get::<OdooDB>(
            &odoo.name_unchecked(),
            odoo
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
        )
        .await
        .context(OdooDBRetrievalSnafu)?;

    tracing::debug!("{}", format!("Checking status: {:#?}", odoo_db.status));

    // Update the Superset cluster status, only if the controller needs to wait.
    // This avoids updating the status twice per reconcile call. when the DB
    // has a ready condition.
    let db_cond_builder = DbConditionBuilder(odoo_db.status);
    if bool::from(&db_cond_builder) {
        let status = OdooClusterStatus {
            conditions: compute_conditions(
                odoo,
                &[&db_cond_builder, cluster_operation_condition_builder],
            ),
        };

        client
            .apply_patch_status(OPERATOR_NAME, odoo, &status)
            .await
            .context(ApplyStatusSnafu)?;
    }
    Ok(bool::from(&db_cond_builder))
}

struct DbConditionBuilder(Option<OdooDBStatus>);
impl ConditionBuilder for DbConditionBuilder {
    fn build_conditions(&self) -> ClusterConditionSet {
        let (status, message) = if let Some(ref status) = self.0 {
            match status.condition {
                OdooDBStatusCondition::Pending | OdooDBStatusCondition::Initializing => (
                    ClusterConditionStatus::False,
                    "Waiting for OdooDB initialization to complete",
                ),
                OdooDBStatusCondition::Failed => (
                    ClusterConditionStatus::False,
                    "Odoo database initialization failed.",
                ),
                OdooDBStatusCondition::Ready => (
                    ClusterConditionStatus::True,
                    "Odoo database initialization ready.",
                ),
            }
        } else {
            (
                ClusterConditionStatus::Unknown,
                "Waiting for Odoo database initialization to start.",
            )
        };

        let cond = ClusterCondition {
            reason: None,
            message: Some(String::from(message)),
            status,
            type_: ClusterConditionType::Available,
            last_transition_time: None,
            last_update_time: None,
        };

        vec![cond].into()
    }
}

/// Evaluates to true if the DB is not ready yet (the controller needs to wait).
/// Otherwise false.
impl From<&DbConditionBuilder> for bool {
    fn from(cond_builder: &DbConditionBuilder) -> bool {
        if let Some(ref status) = cond_builder.0 {
            match status.condition {
                OdooDBStatusCondition::Pending | OdooDBStatusCondition::Initializing => true,
                OdooDBStatusCondition::Failed => true,
                OdooDBStatusCondition::Ready => false,
            }
        } else {
            true
        }
    }
}