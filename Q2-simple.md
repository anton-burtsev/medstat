Индексы

~~~ sql
CREATE INDEX idx_organization_versions_parent_id
ON public.organization_versions(parent_id, organization_id)
INCLUDE(year,version,is_active);

CREATE INDEX idx_organization_versions_organization_id
ON public.organization_versions(organization_id, parent_id)
INCLUDE(year,version,is_active);
~~~

Запрос

~~~ sql
with recursive
org_version as not materialized (
	select ov.*
	from organization_versions ov
	where year = $$
	and is_active
),
uo_versions as not materialized (
	select
		start_rec.organization_id, start_rec.year, start_rec.version,
		exists(select * from org_version lv where lv.parent_id = start_rec.organization_id) as hasChildren
	from
		org_version start_rec
	where
		start_rec.organization_id in ($$)
	union
	select
		step_rec.organization_id, step_rec.year, step_rec.version,
		exists( select * from org_version lv where lv.parent_id = step_rec.organization_id) as hasChildren
	from
		org_version step_rec
	join uo_versions as uo on
		step_rec.parent_id = uo.organization_id
),
user_organizations as not materialized (
	select organization_id, hasChildren from (
		select
			*,
			dense_rank() over(partition by organization_id order by version desc) n
		from uo_versions
	) where n = 1
)
select distinct u.*
from users u
join employment e on u.id = e.user_id
where e.organization_id in (select uo.organization_id from user_organizations uo where uo.hasChildren is false)
and (cast($$ as text) is null or u.last_name || u.first_name || u.patronymic ilike $$ || $$ || $$)
~~~
