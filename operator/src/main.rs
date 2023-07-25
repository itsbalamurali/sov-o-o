mod utils;
mod rbac;
mod odoo_controller;
mod odoo_db_controller;
mod config;
mod controller_commons;
mod product_logging;


use crate::odoo_controller::AIRFLOW_CONTROLLER_NAME;

use clap::{crate_description, crate_version, Parser};
use futures::StreamExt;
use sovrin_cloud_crd::{
    odoodb::{OdooDB, AIRFLOW_DB_CONTROLLER_NAME},
    OdooCluster, OdooClusterAuthenticationConfig, APP_NAME, OPERATOR_NAME,
};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    commons::authentication::AuthenticationClass,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        batch::v1::Job,
        core::v1::{Secret, Service},
    },
    kube::{
        runtime::{reflector::ObjectRef, watcher, Controller},
        ResourceExt,
    },
    logging::controller::report_controller_reconciled,
    CustomResourceExt,
};
use std::sync::Arc;

mod built_info {
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
    pub const TARGET_PLATFORM: Option<&str> = option_env!("TARGET");
}

#[derive(Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();

    match opts.cmd {
        Command::Crd => {
            OdooCluster::print_yaml_schema()?;
            OdooDB::print_yaml_schema()?;
        }
        Command::Run(ProductOperatorRun {
                         product_config,
                         watch_namespace,
                         tracing_target,
                     }) => {
            stackable_operator::logging::initialize_logging(
                "AIRFLOW_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET_PLATFORM.unwrap_or("unknown target"),
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "deploy/config-spec/properties.yaml",
                "/etc/stackable/odoo-operator/config-spec/properties.yaml",
            ])?;

            let client =
                stackable_operator::client::create_client(Some(OPERATOR_NAME.to_string())).await?;

            let odoo_controller_builder = Controller::new(
                watch_namespace.get_api::<OdooCluster>(&client),
                watcher::Config::default(),
            );

            let odoo_store_1 = odoo_controller_builder.store();
            let odoo_store_2 = odoo_controller_builder.store();
            let odoo_controller = odoo_controller_builder
                .owns(
                    watch_namespace.get_api::<Service>(&client),
                    watcher::Config::default(),
                )
                .owns(
                    watch_namespace.get_api::<StatefulSet>(&client),
                    watcher::Config::default(),
                )
                .shutdown_on_signal()
                .watches(
                    client.get_api::<AuthenticationClass>(&()),
                    watcher::Config::default(),
                    move |authentication_class| {
                        odoo_store_1
                            .state()
                            .into_iter()
                            .filter(move |odoo: &Arc<OdooCluster>| {
                                references_authentication_class(
                                    &odoo.spec.cluster_config.authentication_config,
                                    &authentication_class,
                                )
                            })
                            .map(|odoo| ObjectRef::from_obj(&*odoo))
                    },
                )
                .watches(
                    watch_namespace.get_api::<OdooDB>(&client),
                    watcher::Config::default(),
                    move |odoo_db| {
                        odoo_store_2
                            .state()
                            .into_iter()
                            .filter(move |odoo| {
                                odoo_db.name_unchecked() == odoo.name_unchecked()
                                    && odoo_db.namespace() == odoo.namespace()
                            })
                            .map(|odoo| ObjectRef::from_obj(&*odoo))
                    },
                )
                .run(
                    odoo_controller::reconcile_odoo,
                    odoo_controller::error_policy,
                    Arc::new(odoo_controller::Ctx {
                        client: client.clone(),
                        product_config,
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        &format!("{AIRFLOW_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    );
                });

            let odoo_db_controller_builder = Controller::new(
                watch_namespace.get_api::<OdooDB>(&client),
                watcher::Config::default(),
            );

            let odoo_db_store1 = odoo_db_controller_builder.store();
            let odoo_db_store2 = odoo_db_controller_builder.store();
            let odoo_db_controller = odoo_db_controller_builder
                .shutdown_on_signal()
                .watches(
                    watch_namespace.get_api::<Secret>(&client),
                    watcher::Config::default(),
                    move |secret| {
                        odoo_db_store1
                            .state()
                            .into_iter()
                            .filter(move |odoo_db| {
                                if let Some(n) = &secret.metadata.name {
                                    &odoo_db.spec.credentials_secret == n
                                } else {
                                    false
                                }
                            })
                            .map(|odoo_db| ObjectRef::from_obj(&*odoo_db))
                    },
                )
                // We have to watch jobs so we can react to finished init jobs
                // and update our status accordingly
                .watches(
                    watch_namespace.get_api::<Job>(&client),
                    watcher::Config::default(),
                    move |job| {
                        odoo_db_store2
                            .state()
                            .into_iter()
                            .filter(move |odoo_db| {
                                job.name_unchecked() == odoo_db.name_unchecked()
                                    && job.namespace() == odoo_db.namespace()
                            })
                            .map(|odoo_db| ObjectRef::from_obj(&*odoo_db))
                    },
                )
                .run(
                    odoo_db_controller::reconcile_odoo_db,
                    odoo_db_controller::error_policy,
                    Arc::new(odoo_db_controller::Ctx {
                        client: client.clone(),
                    }),
                )
                .map(|res| {
                    report_controller_reconciled(
                        &client,
                        &format!("{AIRFLOW_DB_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                        &res,
                    )
                });

            futures::stream::select(odoo_controller, odoo_db_controller)
                .collect::<()>()
                .await;
        }
    }

    Ok(())
}

fn references_authentication_class(
    authentication_config: &Option<OdooClusterAuthenticationConfig>,
    authentication_class: &AuthenticationClass,
) -> bool {
    assert!(authentication_class.metadata.name.is_some());

    authentication_config
        .as_ref()
        .and_then(|c| c.authentication_class.as_ref())
        == authentication_class.metadata.name.as_ref()
}