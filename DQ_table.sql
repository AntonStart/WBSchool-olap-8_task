create table report.tareload_dq engine = ReplacingMergeTree order by dt_h as
    select toStartOfHour(dt)                            dt_h
         , count()                                      cnt
         , uniq(tare_id)                                qty_tares
         , countIf(tare_id, employee_id = 0)            cnt_empty_empl
         , countIf(tare_id, wh_id = 0)                  cnt_empty_wh
         , countIf(tare_id, place_cod = 0)              cnt_empty_place
         , countIf(tare_id, lifecycle = '')             cnt_empty_lifecycle
         , countIf(tare_id, lifecycle_dst_type = '')    cnt_empty_lifecycle_dst_type
         , countIf(tare_id, lifecycle_dst = 0)          cnt_empty_lifecycle_dst_office
         , countIf(tare_id, wh_tare_status_type = '')   cnt_empty_wh_tare_status_type
         , countIf(tare_id, wh_tare_entry = '')         cnt_empty_wh_tare_entry
    from report.tareTransfer_loc_crop
    group by toStartOfHour(dt)
    order by toStartOfHour(dt);
