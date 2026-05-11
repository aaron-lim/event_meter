require "test_helper"

class EventMeterKeysTest < EventMeterTest
  Index = Struct.new(:key, keyword_init: true)
  Interval = Struct.new(:param, keyword_init: true)

  def test_builds_rollup_keys
    key = EventMeter::Keys.rollup(
      namespace: "billing_app:event_meter:v1",
      name: "invoice_delivery",
      version: 1,
      every: :minute,
      bucket: utc(2026, 5, 6, 1, 2),
      index: Index.new(key: "provider=postmark")
    )

    assert_equal "billing_app:event_meter:v1:rollup:invoice_delivery:v1:minute:202605060102:provider=postmark", key
  end

  def test_builds_rollup_patterns_and_interval_state_keys
    index = Index.new(key: "provider=postmark")

    assert_equal(
      "billing_app:event_meter:v1:rollup:invoice_delivery:v1:hour:*:provider=postmark",
      EventMeter::Keys.rollup_pattern(
        namespace: "billing_app:event_meter:v1",
        name: "invoice_delivery",
        version: 1,
        every: :hour,
        index: index
      )
    )
    assert_equal(
      "billing_app:event_meter:v1:state:invoice_delivery:v1:interval:customer_id:42",
      EventMeter::Keys.interval_state(
        namespace: "billing_app:event_meter:v1",
        name: "invoice_delivery",
        version: 1,
        definition: Interval.new(param: :customer_id),
        value: 42
      )
    )
  end

  def test_index_key_escape_uses_form_url_encoding
    assert_equal "a+b%3Ac%7Cd%3De%2F", EventMeter::IndexKey.escape("a b:c|d=e/")
  end

  def test_escapes_interval_state_key_parts
    assert_equal(
      "billing_app:event_meter:v1:state:sync%3Arun:v1:interval:remote%3Aaccount:42%3Agoogle",
      EventMeter::Keys.interval_state(
        namespace: "billing_app:event_meter:v1",
        name: "sync:run",
        version: 1,
        definition: Interval.new(param: "remote:account"),
        value: "42:google"
      )
    )
  end

  def test_event_path_names_use_a_long_digest_suffix
    path = EventMeter::PathName.event("Invoice Delivery!!!")

    assert_match(/\Ainvoice-delivery-[0-9a-f]{16}\z/, path)
  end
end
