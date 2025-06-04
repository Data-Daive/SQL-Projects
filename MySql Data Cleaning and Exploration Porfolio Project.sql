-- Data Cleaning In MySql with layoff data from https://www.kaggle.com/datasets/swaptr/layoffs-2022

SELECT *
FROM world_layoffs.layoffs
;
# 2361 rows of record returned, checked the overall structure of the data column by column.


-- Before I begin Cleaning, I create a staging table and clean of on that. So i can have the original raw data safe in case something happens. 
CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs
;

INSERT INTO world_layoffs.layoffs_staging
SELECT *
FROM world_layoffs.layoffs
;

SELECT *
FROM world_layoffs.layoffs_staging
;

-- Now I look for duplicates in data using a CTE and remove them after examinining them. 

WITH CTE_Duplicate AS (
SELECT *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as Occurence
FROM world_layoffs.layoffs_staging 
)
SELECT *
FROM CTE_DUPLICATE
WHERE Occurence > 1
;

-- with the creation of a new column 'Occurence' I needed to create a new table with the new column and later insert the table with the Occurence column.

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `Occurence` int DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM world_layoffs.layoffs_staging2
;

-- Now i insert into the table the new records with the new column 'Occurence'

INSERT INTO world_layoffs.layoffs_staging2
SELECT *, row_number() over(partition by company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as Occurence
FROM world_layoffs.layoffs_staging 
;

SELECT *
FROM world_layoffs.layoffs_staging2
;

-- Examining the result to check if they are clearly duplicates. 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE Occurence > 1
; 
-- With the list of companies in this list of duplicate companies I examine the record before deleting them. 

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company IN ('Casper', 'Cazoo', 'Hibob', 'Wildlife Studios', 'Yahoo');
; -- After examining they are real duplicates and will go ahead and delete them next. 

DELETE FROM world_layoffs.layoffs_staging2
WHERE Occurence > 1
;

SELECT *
FROM world_layoffs.layoffs_staging2
; 
-- Now i delete the column 'Occurence' since i have no use of it again

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN Occurence
;

SELECT *
FROM world_layoffs.layoffs_staging2
;

-- Now I Begin standardizing the data, checking for typos, correcting data types and removing spaces before Strings. 

SELECT company , TRIM(company)
FROM world_layoffs.layoffs_staging2
;

UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company)
;

SELECT `date`, str_to_date(`date`, '%m/%d/%Y')
FROM world_layoffs.layoffs_staging2
;

UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y')
;

ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` Date 
;

SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
;

-- Correcting Typos
UPDATE world_layoffs.layoffs_staging2
SET location = 'Malmo'
WHERE location = 'MalmÃ¶'
;

UPDATE world_layoffs.layoffs_staging2
SET location = 'Düsseldorf'
WHERE location = 'DÃ¼sseldorf'
;

UPDATE world_layoffs.layoffs_staging2
SET location = 'Florianópolis'
WHERE location = 'FlorianÃ³polis'
;

SELECT DISTINCT location
FROM world_layoffs.layoffs_staging2
;

SELECT DISTINCT country
FROM world_layoffs.layoffs_staging2
ORDER BY 1 DESC
;

UPDATE world_layoffs.layoffs_staging2
SET country = 'United States'
WHERE country = 'United States.'
;

SELECT DISTINCT industry
FROM world_layoffs.layoffs_staging2
;

UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto Currency'
WHERE industry = 'CryptoCurrency'
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE industry is null 
or industry = ''
;

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE company IN ('Airbnb', 'Carvana','Juul','Bally''s Interactive')
;

-- After looking through the records of the above countries, i found some fields with same company but is null or '', and I will try to use joins to populate those. 

UPDATE world_layoffs.layoffs_staging2
SET industry = null
WHERE industry = ''
;


SELECT *
FROM world_layoffs.layoffs_staging2 as T1
JOIN world_layoffs.layoffs_staging2 as T2
	USING(company)
WHERE T1.industry is null
and T2.industry is not null
; -- I found some null's in the industry column, whiles their same company had values, so I used the self join and filtered on columns with values and those without and with that used it to update the nulls.

UPDATE world_layoffs.layoffs_staging2 as T1
JOIN world_layoffs.layoffs_staging2 as T2
	USING(company)
SET T1.industry = T2.industry
WHERE T1.industry is null
and T2.industry is not null
;

-- Now I look at Actionable Columns with nulls and '', Since its data about layoffs I'll focus on columns(Percentage_laid_off and total_laid_off )

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off is null 
and percentage_laid_off is null
; -- 361 records have nulls in both percentage and total laid off. 


DELETE 
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off is null
and percentage_laid_off is null
;

SELECT *
FROM world_layoffs.layoffs_staging2
; -- Final table has 1995 records

-- EXploratory Analysis of the layoff Data. 

SELECT company, SUM(total_laid_off) as Sum_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
; -- list of companies with their total  number of laid off ordered from the biggest to the lowest

SELECT industry, SUM(total_laid_off) as Sum_laid_off
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC
; -- List of industries and their todtal laid off


SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC
; -- Order in descending order the countries with their layoffs. 
 
 
SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage 
ORDER BY 2 DESC
; 

SELECT max(`date`), min(`date`)
FROM world_layoffs.layoffs_staging2
; -- To know the time span of this whole dataset.
 
SELECT max(total_laid_off), min(total_laid_off)
FROM world_layoffs.layoffs_staging2
; -- To know the maximum and minimun number that has been laid off at a go. 

SELECT company, max(total_laid_off), min(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;


WITH CTE_YEAR_SORT AS (
SELECT company, year(`date`) as YEARS, SUM(total_laid_off) as SUM_TOTAL, max(total_laid_off) as Max_Laid_Off, min(total_laid_off) as Min_Laid_Off
FROM world_layoffs.layoffs_staging2
GROUP BY company, year(`date`)
HAVING YEARS is not null
ORDER BY 2 ASC
),
CTE_SUM_SORT AS (
SELECT *, dense_rank()over(partition by YEARS order by SUM_TOTAL DESC) as Ranks
FROM CTE_YEAR_SORT
)
SELECT * 
FROM CTE_SUM_SORT
WHERE Ranks <= 5
;
-- Here I use multiple Common Table Expressions to Rank companies partitioning by the year, ordering by the total number of staff laid off and selecting the top 5 of each year group. 

SELECT YEAR(`date`), AVG(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(`date`)
; -- TO find the yearly average number of staff laid off. 

SELECT company, year(`date`) as Jahre, sum(total_laid_off) as Total_layoff
FROM world_layoffs.layoffs_staging2
GROUP BY company, year(`date`)
ORDER BY 2 ASC
;

SELECT *
FROM world_layoffs.layoffs_staging2
;
