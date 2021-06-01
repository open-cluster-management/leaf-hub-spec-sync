package data_types

import policiesv1 "github.com/open-cluster-management/governance-policy-propagator/pkg/apis/policy/v1"

const (
	PolicyMessageType = "PoliciesBundle"
)

type PoliciesBundle struct {
	Policies 			[]*policiesv1.Policy `json:"policies"`
	DeletedPolicies 	[]*policiesv1.Policy `json:"deletedPolicies"`
}

func (bundle *PoliciesBundle) AddPolicy(policy *policiesv1.Policy) {
	bundle.Policies = append(bundle.Policies, policy)
}

func (bundle *PoliciesBundle) AddDeletedPolicy(policy *policiesv1.Policy) {
	bundle.DeletedPolicies = append(bundle.DeletedPolicies, policy)
}