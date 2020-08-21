
event_field_names = (
    "timestamp_str",        "line_item_id",        "request_id",
    "channel_id",           "channel_name",        "media_partner_name",
    "url",                  "impression_id",       "event_type",
    "response",             "user_id",             "reason",
    "reason_details",       "entry_type",          "video_url",
    "adserver_id",          "page_id",             "video_id",
    "player_width",         "player_height",       "pub_id",
    "true_label_ids"
    )

compact_labelmap = {
    'user_id' : 'ui',
    'event_type' : 'et',
    'impression_id': 'ii',
    'request_id': 'ri',
    'entry_type' : 'et',
    'event_type' : 'vt',
    'timestamp_str' : 'ts',
    'line_item_id' : 'li',
    'channel_id' : 'ci',
    'media_partner_name': 'mp',
    'response': 're',
    'reason': 'rc',
    'line_item_responses': 'lr',
    'true_label_ids' : 'tl',
    'page_id' : 'pi',
    'event_list' : 'el',
    'unixtime': 'ut',
    'timestamp' : 'tp'
    }


#
#  Fields that are constant w.r.t. line_item_id
#
query_fixed_fields = (
    "timestamp_str",        
    "request_id",
    "channel_id",           
    "channel_name",        
    "media_partner_name",
    "url",                  
    "impression_id",       
    "event_type",
    "user_id",             
    "entry_type",          
    "video_url",
    "adserver_id",          
    "page_id",             
    "video_id",
    "player_width",         
    "player_height",       
    "pub_id",
    "true_label_ids"
    )




r_compact_labelmap = dict(((v,k) for (k,v) in compact_labelmap.items()))

query_resp_map = {
    "True" : "T",
    "False" : "F",
    "None"  : "N"
}

r_query_resp_map = dict(((v,k) for (k,v) in query_resp_map.items()))


def lir_compact(line_item_resp):
    clir = []
    for (li, resp, reasoncode, reason) in line_item_resp:
        v = [li, query_resp_map.get(resp, "N"), reasoncode]
        if (reason is not None):
            v.append(reason)
        clir.append(v)
    return clir

def lir_expand(line_item_resp):
    elir = []
    for lir in line_item_resp:
        v = [lir[0], r_query_resp_map.get(lir[1], "None"), lir[2]]
        v.append((len(lir)==4) and lir[3] or None)
        elir.append(v)
    return elir


def compact(msg):
    cmsg = dict((compact_labelmap.get(k,k),v) for (k,v) in msg.items())
    clirlab = compact_labelmap["line_item_responses"]
    if (clirlab in cmsg):
        cmsg[clirlab] = lir_compact(cmsg[clirlab])
    return cmsg

def expand(msg):
    clirlab = compact_labelmap["line_item_responses"]
    expmsg = dict((r_compact_labelmap.get(k,k),v) for (k,v) in msg.items())
    if (clirlab in msg):
        expmsg["line_item_responses"] = lir_expand(msg[clirlab])
    return expmsg
