# Permission mapping: how to read it, and what it gets wrong

How far to trust the `CAN_*` / `GET_SECRET` edges this stack builds. For how
they're built and deployed, see README.md "Permission mapping".

## What the edges are

`permission_relationships` draws `(:AWSPrincipal)-[:CAN_READ|CAN_WRITE|...]->(resource)`
when the principal's own IAM policies allow any of an entry's permissions on
that resource (`permission_relationships.yaml`). Each edge carries:

- `has_condition`: `true` if *every* Allow that grants it has a `Condition`;
  `false` if at least one Allow grants it unconditionally.
- `condition_keys`: the condition keys involved, e.g.
  `["secretsmanager:ResourceTag/Orca"]`.
- `conditions`: the raw condition blocks (JSON).

## How to query it

- **Unconditional access only:** add `WHERE NOT r.has_condition`. In the
  validation below, no unconditional edge was wrong.
- **IAM users:** also check the user's groups (see limitation 3):
  ```cypher
  MATCH (u:AWSUser)-[:MEMBER_AWS_GROUP]->(g:AWSGroup)-[r:CAN_READ]->(x)
  ```
- **No edge is not proof of no access**: see the limitations, especially
  conditional Denies and group members.

```cypher
// Who can read a parameter, unconditionally, directly or via a group
MATCH (p:AWSSSMParameter {name: $name})
MATCH (pr:AWSPrincipal)-[r:CAN_READ]->(p) WHERE NOT r.has_condition
RETURN pr.arn AS principal, 'direct' AS via
UNION
MATCH (p:AWSSSMParameter {name: $name})
MATCH (u:AWSUser)-[:MEMBER_AWS_GROUP]->(g:AWSGroup)-[r:CAN_READ]->(p) WHERE NOT r.has_condition
RETURN u.arn AS principal, 'group ' + g.name AS via
```

## Validation

**Internal checks** worth running after a full sync:
- Every edge's `lastupdated` matches the latest run (no stale edges).
- Every edge points to a resource in the principal's own account.
- An admin SSO role (e.g. `AWSReservedSSO_AWSAdministratorAccess_*`) can read
  every SSM parameter in its account.

**Compared with the AWS IAM policy simulator.** Run `aws iam
simulate-principal-policy` for every role and user in an account, as the
read-only role. The simulator evaluates identity-based policies only, the
same scope as the engine. One run against a production and a development
account:

| Check | Principals | Agree | Graph no, AWS yes | Graph yes, AWS no |
|---|---|---|---|---|
| Production, 1 SSM parameter, `CAN_READ` | 131 | 130 | 1 | 0 |
| Development, 1 SSM parameter, `CAN_READ` | 278 | 276 | 2 | 0 |
| Development, 1 secret, `GET_SECRET` | 278 | 273 | 1 | 4 |

All 8 mismatches are explained by limitations 1-3 below.

## Known limitations

### 1. A conditional Deny is treated as an absolute deny (under-reports)

The engine ignores the `Condition` on a Deny and applies it everywhere.
Upstream documents this ("a conditional Deny is currently treated as an
absolute deny").

**Typical case: tag-protected secrets.** A common pattern is an elevated SSO
role with `PowerUserAccess` plus Denies that apply only to resources carrying
a protection tag, here `<protect-tag>=true`:
- **SSM parameters:** `ssm:GetParameter`, `GetParameters`,
  `GetParameterHistory`, `PutParameter`, `DeleteParameter(s)`,
  `(Un)LabelParameterVersion`, if `aws:ResourceTag/<protect-tag> = true`.
- **Secrets:** `GetSecretValue`, `PutSecretValue`, `UpdateSecret`,
  `DeleteSecret`, rotation, resource policy and replication actions, same
  condition.
- **Tag tampering:** denies adding or removing the tag on parameters and
  secrets.

AWS: the role can read every *untagged* parameter or secret. Graph: it can
read *none*. 3 of the 8 mismatches.

**Is the tag in the graph?**
- **Secrets: yes.** `(:AWSSecretsManagerSecret)-[:TAGGED]->(:AWSTag)`, written
  by `resourcegroupstaggingapi`.
- **SSM parameters: yes, with the fork.** Same shape:
  `(:AWSSSMParameter)-[:TAGGED]->(:AWSTag)`. Upstream cartography doesn't tag
  parameters (`ssm:parameter` isn't in `AWS_TAGGABLE_RESOURCES`); the fork
  adds that one line, so the existing `resourcegroupstaggingapi` pass picks
  them up with no extra API calls. Upstream PR #3386 takes a different route
  (a `tags` JSON property via `ListTagsForResource` per parameter).

Workaround until the engine handles conditional Denies:
```cypher
// What the elevated role can actually read: untagged secrets and parameters
MATCH (a:AWSAccount)-[:RESOURCE]->(r:AWSRole)
WHERE r.name STARTS WITH $role_prefix
MATCH (a)-[:RESOURCE]->(x)
WHERE (x:AWSSecretsManagerSecret OR x:AWSSSMParameter)
  AND NOT (x)-[:TAGGED]->(:AWSTag {key: $protect_tag, value: 'true'})
RETURN a.name, r.name, labels(x)[0] AS type, count(x) AS readable
```

Caveat: this assumes the role's other policies grant the read
(`PowerUserAccess` does).

### 2. A conditional Allow becomes an edge with `has_condition = true`

By design: the engine can't evaluate request-time conditions, so it draws
the edge and flags it. Example: a role that can read only secrets tagged
`Orca`, or only specific secret IDs. The graph has `GET_SECRET` edges to
*every* secret, flagged, with `condition_keys` naming the tag or ID; AWS
allows only the matching ones. 4 of the 8 mismatches. Filter with
`NOT r.has_condition`.

For tag conditions, you can resolve some yourself, e.g. for
`secretsmanager:ResourceTag/Orca`, check the secret's `TAGGED` edge.

### 3. IAM users get no edges from their groups' policies (under-reports)

The engine evaluates each principal's *own* policies. A group with policies
gets its own `CAN_*` edges, but its member users don't inherit them. Example:
a user who reads SSM only through a group with `AmazonSageMakerFullAccess`
gets no user edge. 1 of the 8 mismatches. Query through `MEMBER_AWS_GROUP`
(see above).

### 4. Not modeled at all

- **SCPs:** synced (`AWSServiceControlPolicy`) but not applied.
- **Permission boundaries.**
- **Resource-based policies:** S3 bucket policies, secret resource policies,
  KMS key policies. For `CAN_DECRYPT` this matters most: the key policy is
  the main control, so read `CAN_DECRYPT` as "IAM allows it".
- **Cross-account access:** only principals in the resource's own account.
- **Session policies** and role assumption chains (who can assume a role
  that has access).

## TODO

- [ ] **Group inheritance.** Before evaluating, merge each user's groups'
      policies into the user's policy set (handler-side, same pattern as
      the prefilter). Removes limitation 3.
- [ ] **Conditional Deny.** Treat it as "may deny": keep the edge, set
      `has_condition = true`, and record the Deny's condition keys, the same
      way a conditional Allow is handled. Removes limitation 1's false
      negatives; upstream PR candidate.
- [ ] **Upstream PR** for the one-line `ssm:parameter` tagging change.
- [ ] **Resolve tag conditions** where the tag is in the graph: evaluate
      `aws:ResourceTag/<k>` and `<service>:ResourceTag/<k>` against `TAGGED`
      edges, and drop or keep the edge accordingly.
