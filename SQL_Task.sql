-- первое задание
SELECT count(title) AS movies_number, c."name" AS category
FROM film f
INNER JOIN film_category fc 
ON fc.film_id = f.film_id 
INNER JOIN category c
ON c.category_id = fc.category_id 
GROUP BY category
ORDER BY movies_number DESC;

--второе задание
SELECT first_name, last_name
FROM actor a
INNER JOIN film_actor fa 
ON a.actor_id = fa.actor_id
INNER JOIN film f
ON fa.film_id = f.film_id
ORDER BY rental_rate DESC 
LIMIT 10;
--третье задание
SELECT name, p.amount
FROM category c
INNER JOIN film_category fc 
ON c.category_id = fc.category_id
INNER JOIN film f 
ON fc.film_id = f.film_id
INNER JOIN inventory i
ON f.film_id = i.film_id
INNER JOIN rental r
ON i.inventory_id = r.inventory_id
INNER JOIN payment p
ON r.rental_id = p.rental_id 
ORDER BY p.amount DESC
LIMIT 1;
--четвертое задание
SELECT title
FROM film f
LEFT JOIN inventory i
ON f.film_id = i.film_id
WHERE inventory_id IS NULL;

--пятое задание

WITH cte_actor_children_category AS (
	SELECT a.first_name || ' ' || a.last_name AS full_name, count(f.title) AS count_title, a.actor_id, c.name
	FROM actor a
	INNER JOIN film_actor fa 
	ON a.actor_id = fa.actor_id
	INNER JOIN film f 
	ON fa.film_id = f.film_id
	LEFT JOIN film_category fc 
	ON f.film_id = fc.film_id
	LEFT JOIN category c 
	ON fc.category_id = c.category_id
	WHERE c.name = 'Children'
	GROUP BY full_name, a.actor_id, c.name
), ranked AS (
	SELECT *, dense_rank() OVER(ORDER BY count_title desc) AS rnk
	FROM cte_actor_children_category
)
SELECT full_name,count_title, rnk 
FROM ranked 
WHERE rnk <= 3 
ORDER BY rnk asc, full_name;

-- шестое задание

SELECT c.city,  sum(cu.active) AS active, count(cu.customer_id) - sum(cu.active) AS inactive 
FROM city c 
INNER JOIN address a 
ON c.city_id = a.city_id
INNER JOIN customer cu
ON a.address_id = cu.address_id
GROUP BY c.city 
ORDER BY inactive desc;

-- седьмое задание

SELECT name
FROM category c
LEFT JOIN film_category fc 
ON c.category_id = fc.category_id
LEFT JOIN film f 
ON fc.film_id = f.film_id
LEFT JOIN inventory i 
ON f.film_id = i.film_id
LEFT JOIN rental r 
ON i.inventory_id = r.inventory_id
LEFT JOIN  customer c2 
ON r.customer_id = c2.customer_id
LEFT JOIN address a 
ON c2.address_id = a.address_id
LEFT JOIN city c3 
ON a.city_id = c3.city_id
WHERE (c3.city LIKE 'A%' OR c3.city LIKE '%-%') 
GROUP BY c.name
ORDER BY SUM(EXTRACT(EPOCH FROM (r.return_date - r.rental_date)) / 3600.0) DESC NULLS LAST

	--восьмое задание

SELECT city,
       category_name,
       category_rental_hours
FROM (
    SELECT
        city.city AS city,
        category.name AS category_name,
        SUM(EXTRACT(EPOCH FROM (rental.return_date - rental.rental_date)) / 3600) AS category_rental_hours,
        dense_rank() OVER (PARTITION BY city.city ORDER BY SUM(EXTRACT(EPOCH FROM (rental.return_date - rental.rental_date)) / 3600) DESC NULLS LAST) AS dense_rank
    FROM city
    LEFT JOIN address        ON address.city_id = city.city_id
    LEFT JOIN customer       ON customer.address_id = address.address_id
    LEFT JOIN rental         ON rental.customer_id = customer.customer_id
    LEFT JOIN inventory      ON inventory.inventory_id = rental.inventory_id
    LEFT JOIN film           ON film.film_id = inventory.film_id
    LEFT JOIN film_category  ON film_category.film_id = film.film_id
    LEFT JOIN category       ON category.category_id = film_category.category_id
    WHERE city.city LIKE 'A%' OR city.city LIKE '%-%'
    GROUP BY city.city, category.name
) hours_ranked
WHERE dense_rank = 1
ORDER BY category_rental_hours;




