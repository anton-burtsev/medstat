1. Создать таблицу актуальных версий organization_versions_actual

~~~ sql
create table organization_versions_actual as
select *
from organization_versions ov
where
	(ov.organization_id,
	ov.year,
	ov.version) in (
	select
		in_ov_max.organization_id,
		in_ov_max.year,
		max(in_ov_max.version) as version
	from
		organization_versions in_ov_max
	group by
		in_ov_max.organization_id,
		in_ov_max.year);

alter table organization_versions_actual
add constraint organization_versions_actual_pk
primary key (organization_id, year);
~~~

2. Создать триггеры

~~~ sql
create or replace function tr_organization_versions_actual_renew()
   returns trigger
   language PLPGSQL
as $$
declare
	t_organization_id bigint;
begin
	if ( TG_OP != 'DELETE' ) then
		insert into organization_versions_actual
		select * from organization_versions ov
		where ov.year = new.year and ov.organization_id = new.organization_id
		order by version desc limit 1
		on conflict(year,organization_id) do update set
			id=EXCLUDED.id, full_name=EXCLUDED.full_name, short_name=EXCLUDED.short_name, inn=EXCLUDED.inn, kpp=EXCLUDED.kpp, ogrn=EXCLUDED.ogrn, region_id=EXCLUDED.region_id, agency_kind_id=EXCLUDED.agency_kind_id, ownership_form_id=EXCLUDED.ownership_form_id, department_id=EXCLUDED.department_id, created=EXCLUDED.created, modified=EXCLUDED.modified, parent_oid=EXCLUDED.parent_oid, profile_agency_kind_id=EXCLUDED.profile_agency_kind_id, delete_date=EXCLUDED.delete_date, relation_type_id=EXCLUDED.relation_type_id, subordination_category_id=EXCLUDED.subordination_category_id, org_level=EXCLUDED.org_level, parent_id=EXCLUDED.parent_id, medstat1_id=EXCLUDED.medstat1_id, organization_type_id=EXCLUDED.organization_type_id, "version"=EXCLUDED."version", regional_type=EXCLUDED.regional_type, num=EXCLUDED.num, is_active=EXCLUDED.is_active, prev_org_id=EXCLUDED.prev_org_id, is_revoked=EXCLUDED.is_revoked, cabinet_id=EXCLUDED.cabinet_id;
		return new;
	else
		insert into organization_versions_actual
		select * from organization_versions ov
		where ov.year = old.year and ov.organization_id = old.organization_id
		order by version desc limit 1
		on conflict(year,organization_id) do update set
			id=EXCLUDED.id, full_name=EXCLUDED.full_name, short_name=EXCLUDED.short_name, inn=EXCLUDED.inn, kpp=EXCLUDED.kpp, ogrn=EXCLUDED.ogrn, region_id=EXCLUDED.region_id, agency_kind_id=EXCLUDED.agency_kind_id, ownership_form_id=EXCLUDED.ownership_form_id, department_id=EXCLUDED.department_id, created=EXCLUDED.created, modified=EXCLUDED.modified, parent_oid=EXCLUDED.parent_oid, profile_agency_kind_id=EXCLUDED.profile_agency_kind_id, delete_date=EXCLUDED.delete_date, relation_type_id=EXCLUDED.relation_type_id, subordination_category_id=EXCLUDED.subordination_category_id, org_level=EXCLUDED.org_level, parent_id=EXCLUDED.parent_id, medstat1_id=EXCLUDED.medstat1_id, organization_type_id=EXCLUDED.organization_type_id, "version"=EXCLUDED."version", regional_type=EXCLUDED.regional_type, num=EXCLUDED.num, is_active=EXCLUDED.is_active, prev_org_id=EXCLUDED.prev_org_id, is_revoked=EXCLUDED.is_revoked, cabinet_id=EXCLUDED.cabinet_id
		returning organization_id into t_organization_id;
		if t_organization_id is null then
			delete from organization_versions_actual ov
			where ov.year = old.year and ov.organization_id = old.organization_id;
		end if;
		return old;
	end if;
end;
$$

create trigger tr_organization_versions_actual
after update or insert or delete on organization_versions
for each row
execute function tr_organization_versions_actual_renew();
~~~

3. Создать индексы

~~~ sql
create index ix_organization_versions_actual_organization_id
on organization_versions_actual(organization_id, year)
include(parent_id,is_active);

create index ix_organization_versions_actual_parent_id
on organization_versions_actual(parent_id, year)
include(organization_id, is_active);

create index ix_employment_org_id
on employment (organization_id, user_id);
~~~


4. Исправить запрос

~~~ sql
begin;

SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;


with recursive
last_version as not materialized (
	select ov.*
	from organization_versions_actual ov
	where year = $$
	and is_active	
),
user_organizations as not materialized (
	select
		start_rec.organization_id,
		exists(select * from last_version lv where lv.parent_id = start_rec.organization_id) as hasChildren
	from
		last_version start_rec
	where
		start_rec.organization_id in ($$)
	union
	select
		step_rec.organization_id,
		exists( select * from last_version lv where lv.parent_id = step_rec.organization_id) as hasChildren
	from
		last_version step_rec
	join user_organizations as c on
		step_rec.parent_id = c.organization_id
)
select
	distinct u.*
from
	users u
join employment e on
	u.id = e.user_id
where
	e.organization_id in (
	select
		uo.organization_id
	from
		user_organizations uo
	where
		uo.hasChildren is false);
	and (cast($3 as text) is null
		or u.last_name || u.first_name || u.patronymic ilike $7 || $4 || $8)

commit;
~~~
Транзакция нужна для того, чтобы ограничить скоуп для SET LOCAL, иначе они могут примениться к другим запросам и всё сломать.

Контролируем, что всё хорошо по вот такому плану выполнения
**Execution Time** должен быть в пределах 1 мс.

~~~
QUERY PLAN                                                                                                                                                                                                               |
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
Unique  (cost=49819.40..51969.21 rows=45259 width=254) (actual time=0.240..0.265 rows=3 loops=1)                                                                                                                         |
  CTE user_organizations                                                                                                                                                                                                 |
    ->  Recursive Union  (cost=0.42..14850.41 rows=27361 width=9) (actual time=0.082..0.102 rows=1 loops=1)                                                                                                              |
          ->  Index Only Scan using ix_organization_versions_actual_organization_id on organization_versions_actual ov_2  (cost=0.42..4.91 rows=1 width=9) (actual time=0.076..0.079 rows=1 loops=1)                     |
                Index Cond: ((organization_id = 7043361) AND (year = 2024))                                                                                                                                              |
                Filter: is_active                                                                                                                                                                                        |
                Heap Fetches: 0                                                                                                                                                                                          |
                SubPlan 1                                                                                                                                                                                                |
                  ->  Index Only Scan using ix_organization_versions_actual_parent_id on organization_versions_actual ov  (cost=0.42..13.90 rows=274 width=0) (actual time=0.020..0.021 rows=0 loops=1)                  |
                        Index Cond: ((parent_id = ov_2.organization_id) AND (year = 2024))                                                                                                                               |
                        Filter: is_active                                                                                                                                                                                |
                        Heap Fetches: 0                                                                                                                                                                                  |
          ->  Nested Loop  (cost=0.42..1457.19 rows=2736 width=9) (actual time=0.015..0.016 rows=0 loops=1)                                                                                                              |
                ->  WorkTable Scan on user_organizations c  (cost=0.00..0.20 rows=10 width=8) (actual time=0.001..0.002 rows=1 loops=1)                                                                                  |
                ->  Index Only Scan using ix_organization_versions_actual_parent_id on organization_versions_actual ov_3  (cost=0.42..13.90 rows=274 width=16) (actual time=0.011..0.011 rows=0 loops=1)                 |
                      Index Cond: ((parent_id = c.organization_id) AND (year = 2024))                                                                                                                                    |
                      Filter: is_active                                                                                                                                                                                  |
                      Heap Fetches: 0                                                                                                                                                                                    |
                SubPlan 3                                                                                                                                                                                                |
                  ->  Index Only Scan using ix_organization_versions_actual_parent_id on organization_versions_actual ov_1  (cost=0.42..13.90 rows=274 width=0) (never executed)                                         |
                        Index Cond: ((parent_id = ov_3.organization_id) AND (year = 2024))                                                                                                                               |
                        Filter: is_active                                                                                                                                                                                |
                        Heap Fetches: 0                                                                                                                                                                                  |
  ->  Sort  (cost=34969.00..35082.14 rows=45259 width=254) (actual time=0.238..0.242 rows=3 loops=1)                                                                                                                     |
        Sort Key: u.id, u.oid, u.last_name, u.first_name, u.patronymic, u.snils, u.email, u.phone, u.created, u.modified, u.role, u.last_login, u.last_sync, u.tg_user_id, u.tg_user_name, u.is_active, u.status, u.roles|
        Sort Method: quicksort  Memory: 25kB                                                                                                                                                                             |
        ->  Nested Loop  (cost=582.26..26054.64 rows=45259 width=254) (actual time=0.168..0.199 rows=3 loops=1)                                                                                                          |
              ->  Nested Loop  (cost=581.84..1395.92 rows=45259 width=4) (actual time=0.139..0.147 rows=3 loops=1)                                                                                                       |
                    ->  HashAggregate  (cost=581.42..583.42 rows=200 width=8) (actual time=0.115..0.118 rows=1 loops=1)                                                                                                  |
                          Group Key: uo.organization_id                                                                                                                                                                  |
                          Batches: 1  Memory Usage: 40kB                                                                                                                                                                 |
                          ->  CTE Scan on user_organizations uo  (cost=0.00..547.22 rows=13680 width=8) (actual time=0.088..0.106 rows=1 loops=1)                                                                        |
                                Filter: (haschildren IS FALSE)                                                                                                                                                           |
                    ->  Index Only Scan using ix_employment_org_id on employment e  (cost=0.42..4.03 rows=3 width=12) (actual time=0.023..0.025 rows=3 loops=1)                                                          |
                          Index Cond: (organization_id = uo.organization_id)                                                                                                                                             |
                          Heap Fetches: 0                                                                                                                                                                                |
              ->  Index Scan using person_pk on users u  (cost=0.42..0.54 rows=1 width=254) (actual time=0.013..0.013 rows=1 loops=3)                                                                                    |
                    Index Cond: (id = e.user_id)                                                                                                                                                                         |
Planning Time: 2.752 ms                                                                                                                                                                                                  |
Execution Time: 0.673 ms                                                                                                                                                                                                 |
~~~
