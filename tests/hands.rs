//! What the placement's hands make of an adapter's answer (docs/api.md, 「Adapters」).

use bmo_arbiter::hands::answer_of;

#[test]
fn an_adapters_answer_is_its_last_line() {
  let answer = answer_of(
    "installing\n{\"ok\":true,\"node\":\"node-a\",\"state\":\"running\",\"detail\":\"\"}\n",
    0,
    "",
  );
  assert!(answer.ok);
  assert_eq!(answer.state.as_deref(), Some("running"));
  let refused = answer_of("halfway\n", 1, "boom");
  assert!(!refused.ok);
  assert!(refused.detail.contains("boom"), "{}", refused.detail);
}
