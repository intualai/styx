# styx

A CLI tool to codegen SQL migrations.

## Key Features

1. Describe your intended database state as a single SQL file
2. Codegen a `golang-migrate` compatible migration by diffing current state -> intended state
3. Fail CICD if a developer modified the schema but forgot to generate migrations
