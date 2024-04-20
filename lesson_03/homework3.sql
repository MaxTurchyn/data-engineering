/*
 Завдання на SQL до лекції 03.
 */


/*
1.
Вивести кількість фільмів в кожній категорії.
Результат відсортувати за спаданням.
*/

SELECT
	  fc.category_id,
	  c."name",
	  count(distinct film_id)
FROM film_category fc
LEFT JOIN category c
ON c.category_id = fc.category_id
GROUP BY 1,2
ORDER BY 3 DESC;

/*
2.
Вивести 10 акторів, чиї фільми брали на прокат найбільше.
Результат відсортувати за спаданням.
*/
-- SQL code goes here...
WITH rentals_by_film AS (
SELECT
	  i.film_id,
	  count(rental_id) rentals_count
FROM rental r
LEFT JOIN inventory i
ON r.inventory_id = i.inventory_id
GROUP BY 1
)

SELECT
	  fa.actor_id,
	  a.first_name,
	  a.last_name,
	  SUM(rentals_count) rentals_sum
FROM film_actor fa
LEFT JOIN rentals_by_film f
ON f.film_id = fa.film_id
LEFT JOIN actor a
ON a.actor_id = fa.actor_id
GROUP BY 1,2,3
ORDER BY 4 DESC

LIMIT 10;

/*
3.
Вивести категорія фільмів, на яку було витрачено найбільше грошей
в прокаті
*/
WITH amount_by_film AS (
SELECT
	  i.film_id,
	  SUM(p.amount) amount
FROM payment p
LEFT JOIN rental r
ON p.rental_id = r.rental_id
LEFT JOIN inventory i
ON i.inventory_id = r.inventory_id
GROUP BY 1
)
SELECT
	  fc.category_id,
	  c.name,
	  SUM(a.amount) amount
FROM amount_by_film a
LEFT JOIN film_category fc
ON a.film_id = fc.film_id
LEFT JOIN category c
ON fc.category_id = c.category_id
GROUP BY 1,2
ORDER BY 3 DESC

/*
4.
Вивести назви фільмів, яких не має в inventory.
Запит має бути без оператора IN
*/
WITH inventory_films AS (
SELECT DISTINCT film_id
FROM inventory
)
SELECT
 	  title
FROM film f
LEFT JOIN inventory_films i
ON f.film_id = i.film_id
WHERE i.film_id IS NULL


/*
5.
Вивести топ 3 актори, які найбільше зʼявлялись в категорії фільмів “Children”.
*/
SELECT
	  actor_id,
	  count(DISTINCT fa.film_id)
FROM film_actor fa
LEFT JOIN film_category fc
ON fa.film_id = fc.film_id
WHERE fc.category_id = 3
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3