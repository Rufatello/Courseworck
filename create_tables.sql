CREATE DATABASE coursework;

CREATE DATABASE coursework;

CREATE TABLE companies (
	company_id int PRIMARY KEY,
	name_company varchar(300)
);
CREATE TABLE job (
	job_id int PRIMARY KEY,
	company_id int REFERENCES companies(company_id),
	job_title varchar(300),
	solary_ot int,
	solary_do int,
	link_vakansy varchar(500)
);

SELECT* FROM companies INNER JOIN job USING(company_id)

SELECT AVG(solary_do) as solary_avg_do FROM job

SELECT * FROM job WHERE solary_do > (SELECT AVG(solary_do) FROM job)

SELECT * FROM job WHERE job_title = '{self.a}'

SELECT companies.name_company, COUNT(job.job_id)
FROM companies
LEFT JOIN job ON companies.company_id = job.company_id
GROUP BY companies.name_company