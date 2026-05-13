//! check_range single-point address bounds (Item #18 regression)

use rs_modbus::utils::check_range;

#[test]
fn single_segment_single_point_value_at_point_passes() {
    assert!(check_range(&[5], &[(5, 5)]));
}

#[test]
fn single_segment_single_point_value_outside_rejects() {
    assert!(!check_range(&[999], &[(5, 5)]));
}

#[test]
fn single_segment_normal_range_still_works() {
    assert!(check_range(&[5], &[(0, 10)]));
    assert!(!check_range(&[999], &[(0, 10)]));
}

#[test]
fn multi_segment_with_single_point_value_at_single_point_passes() {
    assert!(check_range(&[5], &[(5, 5), (10, 20)],));
}

#[test]
fn multi_segment_with_single_point_value_in_normal_segment_passes() {
    assert!(check_range(&[15], &[(5, 5), (10, 20)],));
}

#[test]
fn multi_segment_with_single_point_value_outside_all_rejects() {
    assert!(!check_range(&[999], &[(5, 5), (10, 20)],));
}

#[test]
fn interval_against_single_point_range() {
    assert!(check_range(&[5, 5], &[(5, 5)]));
    assert!(!check_range(&[5, 6], &[(5, 5)]));
}
