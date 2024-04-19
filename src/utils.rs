use rand::{thread_rng, Rng};

pub fn random_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

pub fn take_upto<'a, const N: usize>(data: &mut &'a [u8]) -> Option<&'a [u8; N]> {
    match data.split_first_chunk::<N>() {
        Some((left, right)) => {
            *data = right;
            Some(left)
        }
        None => None,
    }
}
