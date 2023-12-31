use stackable_operator::{
    commons::affinity::{
        affinity_between_cluster_pods, affinity_between_role_pods, StackableAffinityFragment,
    },
    k8s_openapi::api::core::v1::{PodAffinity, PodAntiAffinity},
};

use crate::{OdooRole, APP_NAME};

pub fn get_affinity(cluster_name: &str, role: &OdooRole) -> StackableAffinityFragment {
    let affinity_between_cluster_pods = affinity_between_cluster_pods(APP_NAME, cluster_name, 20);
    let affinity_between_role_pods =
        affinity_between_role_pods(APP_NAME, cluster_name, &role.to_string(), 70);

    StackableAffinityFragment {
        pod_affinity: Some(PodAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_cluster_pods,
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        pod_anti_affinity: Some(PodAntiAffinity {
            preferred_during_scheduling_ignored_during_execution: Some(vec![
                affinity_between_role_pods,
            ]),
            required_during_scheduling_ignored_during_execution: None,
        }),
        node_affinity: None,
        node_selector: None,
    }
}
#[cfg(test)]
mod tests {

    use rstest::rstest;
    use std::collections::BTreeMap;

    use stackable_operator::{
        commons::affinity::{StackableAffinity, StackableNodeSelector},
        k8s_openapi::{
            api::core::v1::{
                NodeAffinity, NodeSelector, NodeSelectorRequirement, NodeSelectorTerm, PodAffinity,
                PodAffinityTerm, PodAntiAffinity, WeightedPodAffinityTerm,
            },
            apimachinery::pkg::apis::meta::v1::LabelSelector,
        },
        kube::runtime::reflector::ObjectRef,
        role_utils::RoleGroupRef,
    };

    use crate::{OdooCluster, OdooRole};

    #[rstest]
    #[case(OdooRole::Worker)]
    #[case(OdooRole::Scheduler)]
    #[case(OdooRole::Webserver)]
    fn test_affinity_defaults(#[case] role: OdooRole) {
        let input = r#"
        apiVersion: odoo.stackable.tech/v1alpha1
        kind: OdooCluster
        metadata:
          name: odoo
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          executor: CeleryExecutor
          loadExamples: true
          exposeConfig: false
          credentialsSecret: simple-odoo-credentials
          webservers:
            roleGroups:
              default:
                replicas: 1
          workers:
            roleGroups:
              default:
                replicas: 2
          schedulers:
            roleGroups:
              default:
                replicas: 1
        "#;
        let odoo: OdooCluster = serde_yaml::from_str(input).expect("illegal test input");

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&odoo),
            role: role.to_string(),
            role_group: "default".to_string(),
        };

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: None,
            node_selector: None,
            pod_affinity: Some(PodAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "odoo".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "odoo".to_string(),
                                    ),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 20,
                    },
                ]),
            }),
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "odoo".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "odoo".to_string(),
                                    ),
                                    ("app.kubernetes.io/component".to_string(), role.to_string()),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let affinity = odoo
            .merged_config(&role, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }

    #[test]
    fn test_affinity_legacy_node_selector() {
        let input = r#"
        apiVersion: odoo.stackable.tech/v1alpha1
        kind: OdooCluster
        metadata:
          name: odoo
        spec:
          image:
            productVersion: 2.6.1
            stackableVersion: 0.0.0-dev
          executor: CeleryExecutor
          loadExamples: true
          exposeConfig: false
          credentialsSecret: simple-odoo-credentials
          webservers:
            roleGroups:
              default:
                replicas: 1
          workers:
            roleGroups:
              default:
                replicas: 2
          schedulers:
            roleGroups:
              default:
                replicas: 1
                selector:
                  matchLabels:
                    disktype: ssd
                  matchExpressions:
                    - key: topology.kubernetes.io/zone
                      operator: In
                      values:
                        - antarctica-east1
                        - antarctica-west1
        "#;

        let odoo: OdooCluster = serde_yaml::from_str(input).expect("illegal test input");

        let expected: StackableAffinity = StackableAffinity {
            node_affinity: Some(NodeAffinity {
                preferred_during_scheduling_ignored_during_execution: None,
                required_during_scheduling_ignored_during_execution: Some(NodeSelector {
                    node_selector_terms: vec![NodeSelectorTerm {
                        match_expressions: Some(vec![NodeSelectorRequirement {
                            key: "topology.kubernetes.io/zone".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec![
                                "antarctica-east1".to_string(),
                                "antarctica-west1".to_string(),
                            ]),
                        }]),
                        match_fields: None,
                    }],
                }),
            }),
            node_selector: Some(StackableNodeSelector {
                node_selector: BTreeMap::from([("disktype".to_string(), "ssd".to_string())]),
            }),
            pod_affinity: Some(PodAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "odoo".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "odoo".to_string(),
                                    ),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 20,
                    },
                ]),
            }),
            pod_anti_affinity: Some(PodAntiAffinity {
                required_during_scheduling_ignored_during_execution: None,
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    WeightedPodAffinityTerm {
                        pod_affinity_term: PodAffinityTerm {
                            label_selector: Some(LabelSelector {
                                match_expressions: None,
                                match_labels: Some(BTreeMap::from([
                                    ("app.kubernetes.io/name".to_string(), "odoo".to_string()),
                                    (
                                        "app.kubernetes.io/instance".to_string(),
                                        "odoo".to_string(),
                                    ),
                                    (
                                        "app.kubernetes.io/component".to_string(),
                                        OdooRole::Scheduler.to_string(),
                                    ),
                                ])),
                            }),
                            namespace_selector: None,
                            namespaces: None,
                            topology_key: "kubernetes.io/hostname".to_string(),
                        },
                        weight: 70,
                    },
                ]),
            }),
        };

        let rolegroup_ref = RoleGroupRef {
            cluster: ObjectRef::from_obj(&odoo),
            role: OdooRole::Scheduler.to_string(),
            role_group: "default".to_string(),
        };

        let affinity = odoo
            .merged_config(&OdooRole::Scheduler, &rolegroup_ref)
            .unwrap()
            .affinity;

        assert_eq!(affinity, expected);
    }
}