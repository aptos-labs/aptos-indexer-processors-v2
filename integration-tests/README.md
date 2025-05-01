## Run tests
While Docker is running, run
```
cargo test sdk_tests
```

## Generate db expected output
```
cargo test sdk_tests -- --nocapture generate
```

