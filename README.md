"# DEMO" 

declare @in_wh_id nvarchar(10)='JAX'

create table #tmp_orders (
	wh_id nvarchar(10),
	order_number nvarchar(30)
)

insert into #tmp_orders (order_number, wh_id)
SELECT DISTINCT pkd.order_number, pkd.wh_id
FROM t_pick_detail pkd WITH (NOLOCK)
INNER JOIN t_order orm WITH (NOLOCK)
	ON pkd.order_number = orm.order_number
	AND pkd.wh_id = orm.wh_id
INNER JOIN t_lookup l WITH (NOLOCK)
	ON l.source = 't_order'
	AND l.lookup_id = orm.type_id
WHERE pkd.wh_id = @in_wh_id											--in this warehouse
	AND pkd.status IN ('RELEASED', 'PICKED', 'STAGED', 'ICNOTIFY')					--released
	AND l.description IN ('PPKMS', 'PPKMSMKT')							--PPKMS orders
	AND orm.department NOT LIKE 'WOS%'									--NOT wholesale
	AND pkd.packed_quantity = 0										--NOT packed
	AND NOT EXISTS (SELECT 1
					FROM t_whse_control whc WITH (NOLOCK)
					WHERE wh_id = @in_wh_id
					AND control_type = 'CUBBIE_CONVEYOR'
					AND pkd.staging_location = whc.c1
					AND EXISTS (SELECT 1
									FROM t_whse_control whc1 WITH (NOLOCK)
									WHERE wh_id = @in_wh_id
									AND control_type = 'PUSH_TO_CONVEYOR'
									AND next_value = 1))

insert into #tmp_orders (order_number, wh_id)
SELECT DISTINCT pkd.order_number, pkd.wh_id
FROM t_pick_detail pkd WITH (NOLOCK)
INNER JOIN t_order orm WITH (NOLOCK)
	ON pkd.order_number = orm.order_number
	AND pkd.wh_id = orm.wh_id
INNER JOIN t_lookup l WITH (NOLOCK)
	ON l.source = 't_order'
	AND l.lookup_id = orm.type_id
WHERE pkd.wh_id = @in_wh_id											--in this warehouse
	AND pkd.status IN ('RELEASED', 'PICKED', 'STAGED', 'ICNOTIFY')					--released
	AND l.description IN ('PPKML', 'PPKMLMKT')							--PPKML orders
	AND orm.department NOT LIKE 'WOS%'									--NOT wholesale
	AND pkd.packed_quantity = 0										--NOT packed
	AND NOT EXISTS (SELECT 1
					FROM t_whse_control whc WITH (NOLOCK)
					WHERE wh_id = @in_wh_id
					AND control_type = 'CUBBIE_CONVEYOR'
					AND pkd.staging_location = whc.c1
					AND EXISTS (SELECT 1
									FROM t_whse_control whc1 WITH (NOLOCK)
									WHERE wh_id = @in_wh_id
									AND control_type = 'PUSH_TO_CONVEYOR'
									AND next_value = 1))

insert into #tmp_orders (order_number, wh_id)
SELECT DISTINCT pkd.order_number, pkd.wh_id
FROM t_pick_detail pkd WITH (NOLOCK)
INNER JOIN t_order orm WITH (NOLOCK)
	ON pkd.order_number = orm.order_number
AND pkd.wh_id = orm.wh_id
INNER JOIN t_lookup l WITH (NOLOCK)
	ON l.source = 't_order'
AND l.lookup_id = orm.type_id
WHERE pkd.wh_id = @in_wh_id											--in this warehouse
AND pkd.status IN ('RELEASED', 'PICKED', 'STAGED', 'ICNOTIFY')					--released
AND l.description IN ('RUSHRK')							--RUSH orders
AND orm.department NOT LIKE 'WOS%'									--NOT wholesale
AND pkd.packed_quantity = 0											--NOT packed
AND NOT EXISTS (SELECT 1
					FROM t_whse_control whc WITH (NOLOCK)
					WHERE wh_id = @in_wh_id
					AND control_type = 'CUBBIE_CONVEYOR'
					AND pkd.staging_location = whc.c1
					AND EXISTS (SELECT 1
									FROM t_whse_control whc1 WITH (NOLOCK)
									WHERE wh_id = @in_wh_id
									AND control_type = 'PUSH_TO_CONVEYOR'
									AND next_value = 1))

insert into #tmp_orders (order_number, wh_id)
SELECT DISTINCT pkd.order_number, pkd.wh_id
FROM t_pick_detail pkd WITH (NOLOCK)
INNER JOIN t_order orm WITH (NOLOCK)
ON pkd.order_number = orm.order_number
AND pkd.wh_id = orm.wh_id
WHERE pkd.wh_id = @in_wh_id											--in this warehouse
AND pkd.status IN ('RELEASED', 'PICKED', 'STAGED', 'ICNOTIFY')					--released
AND orm.department LIKE 'WOS%'										--WOS orders
AND pkd.packed_quantity = 0											--NOT packed
AND NOT EXISTS (SELECT 1
					FROM t_whse_control whc WITH (NOLOCK)
					WHERE wh_id = @in_wh_id
					AND control_type = 'CUBBIE_CONVEYOR'
					AND pkd.staging_location = whc.c1
					AND EXISTS (SELECT 1
									FROM t_whse_control whc1 WITH (NOLOCK)
									WHERE wh_id = @in_wh_id
									AND control_type = 'PUSH_TO_CONVEYOR'
									AND next_value = 1))

begin tran
delete sto
  from t_stored_item sto
 inner join t_pick_detail pkd (nolock)
    ON sto.type = pkd.pick_id
 inner join #tmp_orders tmp (nolock)
    on pkd.order_number = tmp.order_number
   and pkd.wh_id = tmp.wh_id

delete hum
  from t_hu_master hum
 where not exists (select 1
					 from t_stored_item sto (nolock)
					where hum.hu_id = sto.hu_id
					  and hum.wh_id = sto.wh_id)

delete pkd
  from t_pick_detail pkd
 inner join #tmp_orders tmp (nolock)
    on pkd.order_number = tmp.order_number
   and pkd.wh_id = tmp.wh_id

--delete wkq
--  from t_work_q wkq (nolock)
-- where not exists (select 1
--					 from t_pick_detail pkd (nolock)
--					where wkq.work_q_id = pkd.work_q_id)
   
delete pkc
  from t_pick_container pkc
 inner join #tmp_orders tmp (nolock)
    on pkc.order_number = tmp.order_number
   and pkc.wh_id = tmp.wh_id

delete alp
  from t_alp_ship alp
 inner join #tmp_orders tmp (nolock)
    on alp.cust_po_number = tmp.order_number
   and alp.wh_id = tmp.wh_id

delete csr
  from t_usap_centralizedshipping_response csr
 inner join #tmp_orders tmp (nolock)
    on csr.order_id = tmp.order_number
   and csr.wh_id = tmp.wh_id

delete ord
  from t_order_detail ord
 inner join #tmp_orders tmp (nolock)
    on ord.order_number = tmp.order_number
   and ord.wh_id = tmp.wh_id

delete orm
  from t_order orm
 inner join #tmp_orders tmp (nolock)
    on orm.order_number = tmp.order_number
   and orm.wh_id = tmp.wh_id
rollback
--commit

drop table #tmp_orders

select * from t_stored_item where wh_id = 'JAX' nad mmmm
