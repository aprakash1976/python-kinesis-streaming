from affine.model import session, Channel, LineItem
from affine.model.secondary_tables import channel_negative_labels_table
from affine.model.secondary_tables import line_item_negative_labels_table



BRAND_UNSAFE_LABELS = [
    ('IAB:Brand Unsafe', 1054L),
    ('Adult', 994L),
    ('Alcohol', 7585L),
    ('Violence', 7586L),
    ('Drugs', 7587L),
    ('Profanity', 7588L),
]


def get_brand_unsafe_label_ids(line_item_ids):
    """
    brand_safe_query = ""
    self.brand_unsafe_filters.each_with_index do |filter, index|
        brand_safe_query += " || " unless index == 0
        brand_safe_query += "label_#{filter.id}=1"
    end
    brand_safe_query += "label_1054=1" if brand_safe_query == ""
    """

    all_brand_unsafe_label_ids = [_id for name, _id in BRAND_UNSAFE_LABELS]

    channel_ids = session.query(Channel.id).join(LineItem).filter(LineItem.id.in_(line_item_ids)).distinct()

    cnl = channel_negative_labels_table
    query = session.query(cnl.c.label_id)
    query = query.filter(cnl.c.channel_id.in_(channel_ids)).filter(cnl.c.label_id.in_(all_brand_unsafe_label_ids)).distinct()
    brand_unsafe_label_ids = query.all()

    brand_line_item_unsafe_label_ids = []

    lil = line_item_negative_labels_table
    query = session.query(lil.c.label_id).join(LineItem)
    query = query.filter(lil.c.line_item_id.in_(line_item_ids)).filter(lil.c.label_id.in_(all_brand_unsafe_label_ids)).filter(LineItem.is_diagnostic==True).distinct()
    brand_line_item_unsafe_label_ids = query.all()

    # Only set default brand-unsafe filter if none was set
    # by the line items
    # TODO: Confirm this is how brand unsafe should work
    # if not brand_unsafe_label_ids and not brand_line_item_unsafe_label_ids:
    #     return [1054]

    label_ids = [i[0] for i in brand_unsafe_label_ids]
    label_ids.extend([i[0] for i in brand_line_item_unsafe_label_ids])

    return label_ids or all_brand_unsafe_label_ids

