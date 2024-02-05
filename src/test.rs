use regex::Regex;

fn main() {
    let pattern = "sub";  // The substring you are looking for
    let re = Regex::new(pattern).unwrap();

    let test_str = "This is a substring example";

    if re.is_match(test_str) {
        println!("Found a match!");
    } else {
        println!("No match found.");
    }
}

