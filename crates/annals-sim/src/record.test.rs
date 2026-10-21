use super::*;

#[derive(Copy, Clone, PartialEq, Debug)]
enum Side {
    Left,
    Right,
}
rec_enum!(Side: Left, Right);

#[derive(Copy, Clone, PartialEq, Debug)]
struct Id(u32);
rec_newtype!(Id(u32));

fn round_trip<T: Rec + PartialEq + std::fmt::Debug>(x: T) {
    assert_eq!(T::from_value(&x.to_value()), Some(x));
}

#[test]
fn values_round_trip() {
    round_trip(true);
    round_trip(250u8);
    round_trip(u64::MAX);
    round_trip(-7i32);
    round_trip(String::from("Attack (Snap)"));
    round_trip(Some(Id(3)));
    round_trip(None::<Id>);
    round_trip(vec![Side::Left, Side::Right]);
    round_trip((Id(1), Side::Right));
}

#[test]
fn a_wrong_shape_is_refused() {
    assert_eq!(u8::from_value(&Value::Int(300)), None);
    assert_eq!(bool::from_value(&Value::Int(1)), None);
    assert_eq!(Side::from_value(&Value::Str("Up".into())), None);
    assert_eq!(Value::Int(1).get("x"), None);
}

#[test]
fn a_value_reads_back_as_itself() {
    let v = Value::map(vec![("hp", Value::Int(3)), ("up", Value::Bool(true))]);
    round_trip(v.clone());
    assert_eq!(v.field::<Value>("hp"), Some(Value::Int(3)));
}

#[test]
fn a_list_of_the_wrong_length_is_refused() {
    let list = |n: i64| Value::List((0..n).map(Value::Int).collect());
    assert_eq!(Option::<u8>::from_value(&list(2)), None);
    assert_eq!(<(u8, u8)>::from_value(&list(1)), None);
    assert_eq!(<(u8, u8)>::from_value(&list(3)), None);
}
