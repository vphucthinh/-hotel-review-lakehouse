from src.transforms import (add_score_label, clean_reviews, remove_duplicates, split_valid_invalid,
                            tag_aspects)

COLUMNS = ["Hotel_Address", "Hotel_Name", "Average_Score", "Review_Date", "Reviewer_Nationality",
           "Negative_Review", "Positive_Review", "Review_Total_Negative_Word_Counts",
           "Review_Total_Positive_Word_Counts", "Reviewer_Score", "lat", "lng"]


def make_raw(spark, rows):
    return spark.createDataFrame(rows, COLUMNS)


def row(name="Hotel A", date="8/3/2017", neg="Room was small", pos="Great staff", score="8.5",
        address="1 Street London United Kingdom"):
    return (address, name, "8.1", date, " Vietnam ", neg, pos, "3", "2", score, "51.5", "-0.1")


def test_clean_reviews_types_and_placeholders(spark):
    df = clean_reviews(make_raw(spark, [row(neg="No Negative")]))
    r = df.first()
    assert str(r.review_date) == "2017-08-03"
    assert r.reviewer_score == 8.5
    assert r.negative_review == ""
    assert r.reviewer_nationality == "Vietnam"
    assert r.country == "United Kingdom"
    assert len(r.review_id) == 64


def test_country_from_address(spark):
    df = clean_reviews(make_raw(spark, [row(address="5 Rue X Paris France")]))
    assert df.first().country == "France"


def test_remove_duplicates(spark):
    df = remove_duplicates(clean_reviews(make_raw(spark, [row(), row()])))
    assert df.count() == 1


def test_bad_row_goes_to_quarantine_even_if_text_is_duplicated(spark):
    # same text as a good row but wrong score -> must be caught, not dropped as duplicate
    raw = make_raw(spark, [row(), row(score="15")])
    valid, invalid = split_valid_invalid(clean_reviews(raw))
    assert remove_duplicates(valid).count() == 1
    assert invalid.first().dq_reason == "score out of range"


def test_split_valid_invalid(spark):
    raw = make_raw(spark, [row(),
                           row(name="Hotel B", score="12"),
                           row(name="Hotel C", date="not a date"),
                           row(name="Hotel D", neg="No Negative", pos="No Positive")])
    valid, invalid = split_valid_invalid(clean_reviews(raw))
    assert valid.count() == 1
    reasons = sorted(r.dq_reason for r in invalid.collect())
    assert reasons == ["bad review_date", "empty review", "score out of range"]


def test_add_score_label(spark):
    raw = make_raw(spark, [row(score="9"), row(name="B", score="6"), row(name="C", score="4")])
    labels = {r.hotel_name: r.score_label for r in add_score_label(clean_reviews(raw)).collect()}
    assert labels == {"Hotel A": "positive", "B": "neutral", "C": "negative"}


def test_tag_aspects(spark):
    df = clean_reviews(make_raw(spark, [row(neg="The room was dirty", pos="Staff were helpful")]))
    result = {(r.aspect, r.polarity) for r in tag_aspects(df).collect()}
    assert ("room", "negative") in result
    assert ("cleanliness", "negative") in result
    assert ("staff", "positive") in result
    assert ("location", "positive") not in result


def test_tag_aspects_word_start(spark):
    # "barely" should not count as "bar"
    df = clean_reviews(make_raw(spark, [row(neg="barely ok", pos="nice")]))
    assert tag_aspects(df).filter("aspect = 'food'").count() == 0
