use sovrin_cloud_crd::{
    OdooClusterAuthenticationConfig, OdooConfigOptions, LdapRolesSyncMoment,
};
use stackable_operator::commons::authentication::{
    ldap::LdapAuthenticationProvider, tls::TlsVerification, AuthenticationClass,
    AuthenticationClassProvider,
};
use std::collections::BTreeMap;

pub const PYTHON_IMPORTS: &[&str] = &[
    "import os",
    "from odoo.www.fab_security.manager import (AUTH_DB, AUTH_LDAP, AUTH_OAUTH, AUTH_OID, AUTH_REMOTE_USER)",
    "basedir = os.path.abspath(os.path.dirname(__file__))",
    "WTF_CSRF_ENABLED = True",
];

pub fn add_odoo_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: Option<&OdooClusterAuthenticationConfig>,
    authentication_class: Option<&AuthenticationClass>,
) {
    if let Some(authentication_config) = authentication_config {
        if let Some(authentication_class) = authentication_class {
            append_authentication_config(config, authentication_config, authentication_class);
        }
    }
    if !config.contains_key(&*OdooConfigOptions::AuthType.to_string()) {
        config.insert(
            // should default to AUTH_TYPE = AUTH_DB
            OdooConfigOptions::AuthType.to_string(),
            "AUTH_DB".into(),
        );
    }
}

fn append_authentication_config(
    config: &mut BTreeMap<String, String>,
    authentication_config: &OdooClusterAuthenticationConfig,
    authentication_class: &AuthenticationClass,
) {
    if let AuthenticationClassProvider::Ldap(ldap) = &authentication_class.spec.provider {
        append_ldap_config(config, ldap);
    }

    config.insert(
        OdooConfigOptions::AuthUserRegistration.to_string(),
        authentication_config.user_registration.to_string(),
    );
    config.insert(
        OdooConfigOptions::AuthUserRegistrationRole.to_string(),
        authentication_config.user_registration_role.to_string(),
    );
    config.insert(
        OdooConfigOptions::AuthRolesSyncAtLogin.to_string(),
        (authentication_config.sync_roles_at == LdapRolesSyncMoment::Login).to_string(),
    );
}

fn append_ldap_config(config: &mut BTreeMap<String, String>, ldap: &LdapAuthenticationProvider) {
    config.insert(
        OdooConfigOptions::AuthType.to_string(),
        "AUTH_LDAP".into(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapServer.to_string(),
        format!(
            "{protocol}{server_hostname}:{server_port}",
            protocol = match ldap.tls {
                None => "ldap://",
                Some(_) => "ldaps://",
            },
            server_hostname = ldap.hostname,
            server_port = ldap.port.unwrap_or_else(|| ldap.default_port()),
        ),
    );
    config.insert(
        OdooConfigOptions::AuthLdapSearch.to_string(),
        ldap.search_base.clone(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapSearchFilter.to_string(),
        ldap.search_filter.clone(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapUidField.to_string(),
        ldap.ldap_field_names.uid.clone(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapGroupField.to_string(),
        ldap.ldap_field_names.group.clone(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapFirstnameField.to_string(),
        ldap.ldap_field_names.given_name.clone(),
    );
    config.insert(
        OdooConfigOptions::AuthLdapLastnameField.to_string(),
        ldap.ldap_field_names.surname.clone(),
    );

    // Possible TLS options, see https://github.com/dpgaspar/Flask-AppBuilder/blob/f6f66fc1bcc0163a213e4a2e6f960e91082d201f/flask_appbuilder/security/manager.py#L243-L250
    match &ldap.tls {
        None => {
            config.insert(
                OdooConfigOptions::AuthLdapTlsDemand.to_string(),
                false.to_string(),
            );
        }
        Some(tls) => {
            config.insert(
                OdooConfigOptions::AuthLdapTlsDemand.to_string(),
                true.to_string(),
            );
            match &tls.verification {
                TlsVerification::None {} => {
                    config.insert(
                        OdooConfigOptions::AuthLdapAllowSelfSigned.to_string(),
                        true.to_string(),
                    );
                }
                TlsVerification::Server(_) => {
                    config.insert(
                        OdooConfigOptions::AuthLdapAllowSelfSigned.to_string(),
                        false.to_string(),
                    );
                    if let Some(ca_path) = ldap.tls_ca_cert_mount_path() {
                        config.insert(
                            OdooConfigOptions::AuthLdapTlsCacertfile.to_string(),
                            ca_path,
                        );
                    }
                }
            }
        }
    }

    if let Some((username_path, password_path)) = ldap.bind_credentials_mount_paths() {
        config.insert(
            OdooConfigOptions::AuthLdapBindUser.to_string(),
            format!("open('{username_path}').read()"),
        );
        config.insert(
            OdooConfigOptions::AuthLdapBindPassword.to_string(),
            format!("open('{password_path}').read()"),
        );
    }
}

#[cfg(test)]
mod tests {
    use crate::config::add_odoo_config;
    use crate::OdooCluster;
    use sovrin_cloud_crd::LdapRolesSyncMoment::Registration;
    use sovrin_cloud_crd::{OdooClusterAuthenticationConfig, OdooConfigOptions};
    use stackable_operator::commons::authentication::AuthenticationClass;
    use std::collections::BTreeMap;

    #[test]
    fn test_no_ldap() {
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
          ",
        )
            .unwrap();

        let mut result = BTreeMap::new();
        add_odoo_config(
            &mut result,
            cluster.spec.cluster_config.authentication_config.as_ref(),
            None,
        );
        assert_eq!(
            None,
            cluster.spec.cluster_config.authentication_config.as_ref()
        );
        assert_eq!(
            BTreeMap::from([("AUTH_TYPE".into(), "AUTH_DB".into())]),
            result
        );
    }

    #[test]
    fn test_ldap() {
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
            authenticationConfig:
              authenticationClass: odoo-with-ldap-server-veri-tls-ldap
              userRegistrationRole: Admin
          ",
        )
            .unwrap();

        let authentication_class: AuthenticationClass =
            serde_yaml::from_str::<AuthenticationClass>(
                "
            apiVersion: authentication.stackable.tech/v1alpha1
            kind: AuthenticationClass
            metadata:
              name: odoo-with-ldap-server-veri-tls-ldap
            spec:
              provider:
                ldap:
                  hostname: openldap.default.svc.cluster.local
                  port: 636
                  searchBase: ou=users,dc=example,dc=org
                  ldapFieldNames:
                    uid: uid
                  bindCredentials:
                    secretClass: odoo-with-ldap-server-veri-tls-ldap-bind
                  tls:
                    verification:
                      server:
                        caCert:
                          secretClass: openldap-tls
          ",
            )
                .unwrap();

        let mut result = BTreeMap::new();
        add_odoo_config(
            &mut result,
            cluster.spec.cluster_config.authentication_config.as_ref(),
            Some(&authentication_class),
        );
        assert_eq!(
            Some(OdooClusterAuthenticationConfig {
                authentication_class: Some("odoo-with-ldap-server-veri-tls-ldap".to_string()),
                user_registration: true,
                user_registration_role: "Admin".to_string(),
                sync_roles_at: Registration
            }),
            cluster.spec.cluster_config.authentication_config
        );
        assert_eq!(
            "AUTH_LDAP",
            result
                .get(&OdooConfigOptions::AuthType.to_string())
                .unwrap()
        );
        println!("{result:#?}");
    }
}