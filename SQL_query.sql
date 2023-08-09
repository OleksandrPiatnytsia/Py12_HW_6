1.Знайти 5 студентів із найбільшим середнім балом з усіх предметів.
select
	AVG(po.point) as AV_points,
	stud.name
from points as po
left join students as stud
	on po.student_id = stud.id
group by stud.name
order by AV_points desc
limit 5


2.Знайти студента із найвищим середнім балом з певного предмета.
select
	AVG(po.point) as AV_points,
	stud.name,
	sub.name
from points as po
left join students as stud
	on po.student_id = stud.id
left join subjects as sub
	on po.subject_id = sub.id
group by
	stud.name,
	sub.name
order by AV_points desc
limit 1


3.Знайти середній бал у групах з певного предмета.
select
	AVG(po.point) as AV_points,
	g.name
from points as po
left join subjects as sub
	on po.subject_id = sub.id
left join students as stud
	on po.student_id = stud.id
left join groups as g
	on stud.group_id = g.id
where sub.id = 3
group by
	g.name

4. Знайти середній бал на потоці (по всій таблиці оцінок).
select
	AVG(po.point) as AV_points
from points as po

5.Знайти, які курси читає певний викладач.
select
	tchr.name as tchr_name,
	sub.name as sub_name
from subjects as sub
left join teachers as tchr
	on sub.teacher_id = tchr.id
where
	sub.teacher_id = 1

6.Знайти список студентів у певній групі.
select
	g.name as g_name,
	stud.name as stud_name
from students as stud
left join groups as g
	on stud.group_id = g.id
where
	stud.group_id = 2


7.Знайти оцінки студентів в окремій групі з певного предмета.


8.Знайти середній бал, який ставить певний викладач зі своїх предметів.
9.Знайти список курсів, які відвідує студент.
10.Список курсів, які певному студенту читає певний викладач.