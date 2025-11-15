fn main() {
    // Enable the rustler_unstable cfg flag for async NIF support
    println!("cargo:rustc-cfg=rustler_unstable");
}
