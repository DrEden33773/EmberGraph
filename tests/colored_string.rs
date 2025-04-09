use colored::Colorize;

#[test]
fn test_colored_string() {
  let mut string = String::new();
  string.push_str(&"Hello, ".yellow().to_string());
  string.push_str(&"world!".green().to_string());

  println!("{}", string); // with color
}
