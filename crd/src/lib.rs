pub mod affinity;
pub mod odoodb;

use crate::affinity::get_affinity;
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::commons::affinity::StackableAffinity;
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::kube::ResourceExt;
use stackable_operator::memory::{BinaryMultiple, MemoryQuantity};
use stackable_operator::role_utils::RoleGroup;
use stackable_operator::{
    commons::cluster_operation::ClusterOperation,
    commons::resources::{
        CpuLimitsFragment, MemoryLimitsFragment, NoRuntimeLimits, NoRuntimeLimitsFragment,
        Resources, ResourcesFragment,
    },
    config::{fragment, fragment::Fragment, fragment::ValidationError, merge::Merge},
    k8s_openapi::{
        api::core::v1::{Volume, VolumeMount},
        apimachinery::pkg::api::resource::Quantity,
    },
    kube::CustomResource,
    labels::ObjectLabels,
    product_config::flask_app_config_writer::{FlaskAppConfigOptions, PythonType},
    product_config_utils::{ConfigError, Configuration},
    product_logging::{self, spec::Logging},
    role_utils::{Role, RoleGroupRef},
    schemars::{self, JsonSchema},
    status::condition::{ClusterCondition, HasStatusCondition},
};

use std::collections::BTreeMap;
use std::ops::Deref;
use strum::{Display, EnumIter, EnumString, IntoEnumIterator};

pub const AIRFLOW_UID: i64 = 1000;
pub const APP_NAME: &str = "odoo";
pub const OPERATOR_NAME: &str = "odoo.sovrin.cloud";
pub const CONFIG_PATH: &str = "/stackable/app/config";
pub const STACKABLE_LOG_DIR: &str = "/stackable/log";
pub const LOG_CONFIG_DIR: &str = "/stackable/app/log_config";
pub const AIRFLOW_HOME: &str = "/stackable/odoo";
pub const AIRFLOW_CONFIG_FILENAME: &str = "webserver_config.py";
pub const GIT_SYNC_DIR: &str = "/stackable/app/git";
pub const GIT_CONTENT: &str = "content-from-git";
pub const GIT_ROOT: &str = "/tmp/git";
pub const GIT_LINK: &str = "current";
pub const GIT_SYNC_NAME: &str = "gitsync";

const GIT_SYNC_DEPTH: u8 = 1u8;
const GIT_SYNC_WAIT: u16 = 20u16;

pub const MAX_LOG_FILES_SIZE: MemoryQuantity = MemoryQuantity {
    value: 10.0,
    unit: BinaryMultiple::Mebi,
};

#[derive(Snafu, Debug)]
pub enum Error {
    #[snafu(display("Unknown Odoo role found {role}. Should be one of {roles:?}"))]
    UnknownOdooRole { role: String, roles: Vec<String> },
    #[snafu(display("fragment validation failure"))]
    FragmentValidationFailure { source: ValidationError },
}

#[derive(Display, EnumIter, EnumString)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum OdooConfigOptions {
    AuthType,
    AuthLdapSearch,
    AuthLdapSearchFilter,
    AuthLdapServer,
    AuthLdapUidField,
    AuthLdapBindUser,
    AuthLdapBindPassword,
    AuthUserRegistration,
    AuthUserRegistrationRole,
    AuthLdapFirstnameField,
    AuthLdapLastnameField,
    AuthLdapEmailField,
    AuthLdapGroupField,
    AuthRolesSyncAtLogin,
    AuthLdapTlsDemand,
    AuthLdapTlsCertfile,
    AuthLdapTlsKeyfile,
    AuthLdapTlsCacertfile,
    AuthLdapAllowSelfSigned,
}

impl FlaskAppConfigOptions for OdooConfigOptions {
    fn python_type(&self) -> PythonType {
        match self {
            OdooConfigOptions::AuthType => PythonType::Expression,
            OdooConfigOptions::AuthUserRegistration => PythonType::BoolLiteral,
            OdooConfigOptions::AuthUserRegistrationRole => PythonType::StringLiteral,
            OdooConfigOptions::AuthRolesSyncAtLogin => PythonType::BoolLiteral,
            OdooConfigOptions::AuthLdapServer => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapBindUser => PythonType::Expression,
            OdooConfigOptions::AuthLdapBindPassword => PythonType::Expression,
            OdooConfigOptions::AuthLdapSearch => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapSearchFilter => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapUidField => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapGroupField => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapFirstnameField => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapLastnameField => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapEmailField => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapTlsDemand => PythonType::BoolLiteral,
            OdooConfigOptions::AuthLdapTlsCertfile => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapTlsKeyfile => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapTlsCacertfile => PythonType::StringLiteral,
            OdooConfigOptions::AuthLdapAllowSelfSigned => PythonType::BoolLiteral,
        }
    }
}

#[derive(Clone, CustomResource, Debug, Deserialize, JsonSchema, PartialEq, Serialize)]
#[kube(
group = "odoo.stackable.tech",
version = "v1alpha1",
kind = "OdooCluster",
plural = "odooclusters",
shortname = "odoo",
status = "OdooClusterStatus",
namespaced,
crates(
kube_core = "stackable_operator::kube::core",
k8s_openapi = "stackable_operator::k8s_openapi",
schemars = "stackable_operator::schemars"
)
)]
#[serde(rename_all = "camelCase")]
pub struct OdooClusterSpec {
    /// The Odoo image to use
    pub image: ProductImage,
    /// Global cluster configuration that applies to all roles and role groups
    #[serde(default)]
    pub cluster_config: OdooClusterConfig,
    /// Cluster operations like pause reconciliation or cluster stop.
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub webservers: Option<Role<OdooConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedulers: Option<Role<OdooConfigFragment>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workers: Option<Role<OdooConfigFragment>>,
}

#[derive(Clone, Deserialize, Debug, Default, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OdooClusterConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication_config: Option<OdooClusterAuthenticationConfig>,
    pub credentials_secret: String,
    #[serde(default)]
    pub dags_git_sync: Vec<GitSync>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database_initialization: Option<odoodb::OdooDbConfigFragment>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executor: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expose_config: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_examples: Option<bool>,
    /// In the future this setting will control, which ListenerClass <https://docs.stackable.tech/home/stable/listener-operator/listenerclass.html>
    /// will be used to expose the service.
    /// Currently only a subset of the ListenerClasses are supported by choosing the type of the created Services
    /// by looking at the ListenerClass name specified,
    /// In a future release support for custom ListenerClasses will be introduced without a breaking change:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    /// * external-stable: Use a LoadBalancer service
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,
    /// Name of the Vector aggregator discovery ConfigMap.
    /// It must contain the key `ADDRESS` with the address of the Vector aggregator.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_aggregator_config_map_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volumes: Option<Vec<Volume>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_mounts: Option<Vec<VolumeMount>>,
}

// TODO: Temporary solution until listener-operator is finished
#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
    #[serde(rename = "external-stable")]
    ExternalStable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
            CurrentlySupportedListenerClasses::ExternalStable => "LoadBalancer".to_string(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GitSync {
    pub repo: String,
    pub branch: Option<String>,
    pub git_folder: Option<String>,
    pub depth: Option<u8>,
    pub wait: Option<u16>,
    pub credentials_secret: Option<String>,
    pub git_sync_conf: Option<BTreeMap<String, String>>,
}

impl GitSync {
    pub fn get_args(&self) -> Vec<String> {
        let mut args: Vec<String> = vec![];
        args.extend(vec![
            "/stackable/git-sync".to_string(),
            format!("--repo={}", self.repo.clone()),
            format!(
                "--branch={}",
                self.branch.clone().unwrap_or_else(|| "main".to_string())
            ),
            format!("--depth={}", self.depth.unwrap_or(GIT_SYNC_DEPTH)),
            format!("--wait={}", self.wait.unwrap_or(GIT_SYNC_WAIT)),
            format!("--dest={GIT_LINK}"),
            format!("--root={GIT_ROOT}"),
            format!("--git-config=safe.directory:{GIT_ROOT}"),
        ]);
        if let Some(git_sync_conf) = self.git_sync_conf.as_ref() {
            for (key, value) in git_sync_conf {
                // config options that are internal details have
                // constant values and will be ignored here
                if key.eq_ignore_ascii_case("--dest")
                    || key.eq_ignore_ascii_case("--root")
                    || key.eq_ignore_ascii_case("--git-config")
                {
                    tracing::warn!("Config option {:?} will be ignored...", key);
                } else {
                    args.push(format!("{key}={value}"));
                }
            }
        }
        args
    }
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OdooClusterAuthenticationConfig {
    /// Name of the AuthenticationClass used to authenticate the users.
    /// At the moment only LDAP is supported.
    /// If not specified the default authentication (AUTH_DB) will be used.
    pub authentication_class: Option<String>,

    /// Allow users who are not already in the FAB DB.
    /// Gets mapped to `AUTH_USER_REGISTRATION`
    #[serde(default = "default_user_registration")]
    pub user_registration: bool,

    /// This role will be given in addition to any AUTH_ROLES_MAPPING.
    /// Gets mapped to `AUTH_USER_REGISTRATION_ROLE`
    #[serde(default = "default_user_registration_role")]
    pub user_registration_role: String,

    /// If we should replace ALL the user's roles each login, or only on registration.
    /// Gets mapped to `AUTH_ROLES_SYNC_AT_LOGIN`
    #[serde(default = "default_sync_roles_at")]
    pub sync_roles_at: LdapRolesSyncMoment,
}

pub fn default_user_registration() -> bool {
    true
}

pub fn default_user_registration_role() -> String {
    "Public".to_string()
}

/// Matches Flask's default mode of syncing at registration
pub fn default_sync_roles_at() -> LdapRolesSyncMoment {
    LdapRolesSyncMoment::Registration
}

#[derive(Clone, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
pub enum LdapRolesSyncMoment {
    Registration,
    Login,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OdooCredentials {
    pub admin_user: AdminUserCredentials,
    pub connections: Connections,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AdminUserCredentials {
    pub username: String,
    pub firstname: String,
    pub lastname: String,
    pub email: String,
    pub password: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Connections {
    pub secret_key: String,
    pub sqlalchemy_database_uri: String,
}

#[derive(
Clone,
Debug,
Deserialize,
Display,
EnumIter,
Eq,
Hash,
JsonSchema,
PartialEq,
Serialize,
EnumString,
)]
pub enum OdooRole {
    #[strum(serialize = "webserver")]
    Webserver,
    #[strum(serialize = "scheduler")]
    Scheduler,
    #[strum(serialize = "worker")]
    Worker,
}

impl OdooRole {
    /// Returns the start commands for the different odoo components. Odoo expects all
    /// components to have the same image/configuration (e.g. DAG folder location), even if not all
    /// configuration settings are used everywhere. For this reason we ensure that the webserver
    /// config file is in the Odoo home directory on all pods.
    pub fn get_commands(&self) -> Vec<String> {
        let copy_config = format!(
            "cp -RL {CONFIG_PATH}/{AIRFLOW_CONFIG_FILENAME} \
            {AIRFLOW_HOME}/{AIRFLOW_CONFIG_FILENAME}"
        );
        match &self {
            OdooRole::Webserver => vec![copy_config, "odoo webserver".to_string()],
            OdooRole::Scheduler => vec![copy_config, "odoo scheduler".to_string()],
            OdooRole::Worker => vec![copy_config, "odoo celery worker".to_string()],
        }
    }

    /// Will be used to expose service ports and - by extension - which roles should be
    /// created as services.
    pub fn get_http_port(&self) -> Option<u16> {
        match &self {
            OdooRole::Webserver => Some(8080),
            OdooRole::Scheduler => None,
            OdooRole::Worker => None,
        }
    }

    pub fn roles() -> Vec<String> {
        let mut roles = vec![];
        for role in Self::iter() {
            roles.push(role.to_string())
        }
        roles
    }
}

impl OdooCluster {
    pub fn get_role(&self, role: &OdooRole) -> &Option<Role<OdooConfigFragment>> {
        match role {
            OdooRole::Webserver => &self.spec.webservers,
            OdooRole::Scheduler => &self.spec.schedulers,
            OdooRole::Worker => &self.spec.workers,
        }
    }

    /// this will extract a `Vec<Volume>` from `Option<Vec<Volume>>`
    pub fn volumes(&self) -> Vec<Volume> {
        let tmp = self.spec.cluster_config.volumes.as_ref();
        tmp.iter().flat_map(|v| v.deref().clone()).collect()
    }

    pub fn volume_mounts(&self) -> Vec<VolumeMount> {
        let tmp = self.spec.cluster_config.volume_mounts.as_ref();
        let mut mounts: Vec<VolumeMount> = tmp.iter().flat_map(|v| v.deref().clone()).collect();
        if self.git_sync().is_some() {
            mounts.push(VolumeMount {
                name: GIT_CONTENT.into(),
                mount_path: GIT_SYNC_DIR.into(),
                ..VolumeMount::default()
            });
        }
        mounts
    }

    pub fn git_sync(&self) -> Option<&GitSync> {
        let dags_git_sync = &self.spec.cluster_config.dags_git_sync;
        // dags_git_sync is a list but only the first element is considered
        // (this avoids a later breaking change when all list elements are processed)
        if dags_git_sync.len() > 1 {
            tracing::warn!(
                "{:?} git-sync elements: only first will be considered...",
                dags_git_sync.len()
            );
        }
        dags_git_sync.first()
    }
}

#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, Debug, Default, JsonSchema, PartialEq, Fragment)]
#[fragment_attrs(
allow(clippy::derive_partial_eq_without_eq),
derive(
Clone,
Debug,
Default,
Deserialize,
Merge,
JsonSchema,
PartialEq,
Serialize
),
serde(rename_all = "camelCase")
)]
pub struct OdooStorageConfig {}

#[derive(
Clone,
Debug,
Deserialize,
Display,
Eq,
EnumIter,
JsonSchema,
Ord,
PartialEq,
PartialOrd,
Serialize,
)]
#[serde(rename_all = "kebab-case")]
#[strum(serialize_all = "kebab-case")]
pub enum Container {
    Odoo,
    Vector,
}

#[derive(Clone, Debug, Default, Fragment, JsonSchema, PartialEq)]
#[fragment_attrs(
derive(
Clone,
Debug,
Default,
Deserialize,
Merge,
JsonSchema,
PartialEq,
Serialize
),
serde(rename_all = "camelCase")
)]
pub struct OdooConfig {
    #[fragment_attrs(serde(default))]
    pub resources: Resources<OdooStorageConfig, NoRuntimeLimits>,
    #[fragment_attrs(serde(default))]
    pub logging: Logging<Container>,
    #[fragment_attrs(serde(default))]
    pub affinity: StackableAffinity,
}

impl OdooConfig {
    pub const CREDENTIALS_SECRET_PROPERTY: &'static str = "credentialsSecret";
    pub const GIT_CREDENTIALS_SECRET_PROPERTY: &'static str = "gitCredentialsSecret";

    fn default_config(cluster_name: &str, role: &OdooRole) -> OdooConfigFragment {
        let (cpu, memory) = match role {
            OdooRole::Worker => (
                CpuLimitsFragment {
                    min: Some(Quantity("200m".into())),
                    max: Some(Quantity("800m".into())),
                },
                MemoryLimitsFragment {
                    limit: Some(Quantity("1750Mi".into())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
            ),
            OdooRole::Webserver => (
                CpuLimitsFragment {
                    min: Some(Quantity("100m".into())),
                    max: Some(Quantity("400m".into())),
                },
                MemoryLimitsFragment {
                    limit: Some(Quantity("2Gi".into())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
            ),
            OdooRole::Scheduler => (
                CpuLimitsFragment {
                    min: Some(Quantity("100m".to_owned())),
                    max: Some(Quantity("400m".to_owned())),
                },
                MemoryLimitsFragment {
                    limit: Some(Quantity("512Mi".to_owned())),
                    runtime_limits: NoRuntimeLimitsFragment {},
                },
            ),
        };

        OdooConfigFragment {
            resources: ResourcesFragment {
                cpu,
                memory,
                storage: OdooStorageConfigFragment {},
            },
            logging: product_logging::spec::default_logging(),
            affinity: get_affinity(cluster_name, role),
        }
    }
}

impl Configuration for OdooConfigFragment {
    type Configurable = OdooCluster;

    fn compute_env(
        &self,
        cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        let mut env: BTreeMap<String, Option<String>> = BTreeMap::new();
        env.insert(
            OdooConfig::CREDENTIALS_SECRET_PROPERTY.to_string(),
            Some(cluster.spec.cluster_config.credentials_secret.clone()),
        );
        if let Some(git_sync) = &cluster.git_sync() {
            if let Some(credentials_secret) = &git_sync.credentials_secret {
                env.insert(
                    OdooConfig::GIT_CREDENTIALS_SECRET_PROPERTY.to_string(),
                    Some(credentials_secret.to_string()),
                );
            }
        }
        Ok(env)
    }

    fn compute_cli(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _cluster: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, ConfigError> {
        Ok(BTreeMap::new())
    }
}

#[derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OdooClusterStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for OdooCluster {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

impl OdooCluster {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn node_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Retrieve and merge resource configs for role and role groups
    pub fn merged_config(
        &self,
        role: &OdooRole,
        rolegroup_ref: &RoleGroupRef<OdooCluster>,
    ) -> Result<OdooConfig, Error> {
        // Initialize the result with all default values as baseline
        let conf_defaults = OdooConfig::default_config(&self.name_any(), role);

        let role = match role {
            OdooRole::Webserver => {
                self.spec
                    .webservers
                    .as_ref()
                    .context(UnknownOdooRoleSnafu {
                        role: role.to_string(),
                        roles: OdooRole::roles(),
                    })?
            }
            OdooRole::Worker => self
                .spec
                .workers
                .as_ref()
                .context(UnknownOdooRoleSnafu {
                    role: role.to_string(),
                    roles: OdooRole::roles(),
                })?,
            OdooRole::Scheduler => {
                self.spec
                    .schedulers
                    .as_ref()
                    .context(UnknownOdooRoleSnafu {
                        role: role.to_string(),
                        roles: OdooRole::roles(),
                    })?
            }
        };

        // Retrieve role resource config
        let mut conf_role = role.config.config.to_owned();

        // Retrieve rolegroup specific resource config
        let mut conf_rolegroup = role
            .role_groups
            .get(&rolegroup_ref.role_group)
            .map(|rg| rg.config.config.clone())
            .unwrap_or_default();

        if let Some(RoleGroup {
                        selector: Some(selector),
                        ..
                    }) = role.role_groups.get(&rolegroup_ref.role_group)
        {
            // Migrate old `selector` attribute, see ADR 26 affinities.
            // TODO Can be removed after support for the old `selector` field is dropped.
            #[allow(deprecated)]
            conf_rolegroup.affinity.add_legacy_selector(selector);
        }

        // Merge more specific configs into default config
        // Hierarchy is:
        // 1. RoleGroup
        // 2. Role
        // 3. Default
        conf_role.merge(&conf_defaults);
        conf_rolegroup.merge(&conf_role);

        tracing::debug!("Merged config: {:?}", conf_rolegroup);
        fragment::validate(conf_rolegroup).context(FragmentValidationFailureSnafu)
    }
}

/// Creates recommended `ObjectLabels` to be used in deployed resources
pub fn build_recommended_labels<'a, T>(
    owner: &'a T,
    controller_name: &'a str,
    app_version: &'a str,
    role: &'a str,
    role_group: &'a str,
) -> ObjectLabels<'a, T> {
    ObjectLabels {
        owner,
        app_name: APP_NAME,
        app_version,
        operator_name: OPERATOR_NAME,
        controller_name,
        role,
        role_group,
    }
}

/// A reference to a [`OdooCluster`]
#[derive(Clone, Default, Debug, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OdooClusterRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use crate::odoodb::OdooDB;
    use crate::OdooCluster;
    use stackable_operator::commons::product_image_selection::ResolvedProductImage;

    #[test]
    fn test_cluster_config() {
        let cluster: OdooCluster = serde_yaml::from_str::<OdooCluster>(
            "
        apiVersion: odoo.stackable.tech/v1alpha1
        kind: OdooCluster
        metadata:
          name: odoo
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: KubernetesExecutor
            loadExamples: true
            exposeConfig: true
            credentialsSecret: simple-odoo-credentials
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
            .unwrap();

        let resolved_odoo_image: ResolvedProductImage = cluster.spec.image.resolve("odoo");

        let odoo_db = OdooDB::for_odoo(&cluster, &resolved_odoo_image).unwrap();
        let resolved_odoo_db_image: ResolvedProductImage =
            odoo_db.spec.image.resolve("odoo");

        assert_eq!("2.6.1", &resolved_odoo_db_image.product_version);
        assert_eq!("2.6.1", &resolved_odoo_image.product_version);
        assert_eq!(
            "KubernetesExecutor",
            cluster.spec.cluster_config.executor.unwrap_or_default()
        );
        assert!(cluster.spec.cluster_config.load_examples.unwrap_or(false));
        assert!(cluster.spec.cluster_config.expose_config.unwrap_or(false));
    }

    #[test]
    fn test_git_sync() {
        let cluster: OdooCluster = serde_yaml::from_str::<OdooCluster>(
            "
        apiVersion: odoo.stackable.tech/v1alpha1
        kind: OdooCluster
        metadata:
          name: odoo
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: CeleryExecutor
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-odoo-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/odoo-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf: {}
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
            .unwrap();

        assert!(cluster.git_sync().is_some(), "git_sync was not Some!");
        assert_eq!(
            Some("tests/templates/kuttl/mount-dags-gitsync/dags".to_string()),
            cluster.git_sync().unwrap().git_folder
        );
    }

    #[test]
    fn test_git_sync_config() {
        let cluster: OdooCluster = serde_yaml::from_str::<OdooCluster>(
            "
        apiVersion: odoo.stackable.tech/v1alpha1
        kind: OdooCluster
        metadata:
          name: odoo
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          clusterConfig:
            executor: CeleryExecutor
            loadExamples: false
            exposeConfig: false
            credentialsSecret: simple-odoo-credentials
            dagsGitSync:
              - name: git-sync
                repo: https://github.com/stackabletech/odoo-operator
                branch: feat/git-sync
                wait: 20
                gitSyncConf:
                  --rev: c63921857618a8c392ad757dda13090fff3d879a
                gitFolder: tests/templates/kuttl/mount-dags-gitsync/dags
          webservers:
            roleGroups:
              default:
                config: {}
          workers:
            roleGroups:
              default:
                config: {}
          schedulers:
            roleGroups:
              default:
                config: {}
          ",
        )
            .unwrap();

        assert!(cluster
            .git_sync()
            .unwrap()
            .get_args()
            .iter()
            .any(|c| c == "--rev=c63921857618a8c392ad757dda13090fff3d879a"));
    }
}