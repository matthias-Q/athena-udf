work:
    cargo watch -x "check" -s "just test" -s "just lint"
lint:
    cargo clippy
test:
    cargo nextest run --nocapture
publish TYPE="patch" *FLAGS:
    cargo release {{TYPE}} --package athena-udf --exclude simple-udf --exclude manual-udf {{FLAGS}}
