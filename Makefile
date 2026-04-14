RUSTDOCFLAGS="--cfg docsrs"
export RUSTDOCFLAGS

docs:
	RUSTDOCFLAGS=$(RUSTDOCFLAGS) cargo +nightly doc --all-features --no-deps --open
