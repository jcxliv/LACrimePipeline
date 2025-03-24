-- Identifying Missing District Data
SELECT DISTINCT ttd.TRACTCE10, c.District
FROM tract_to_district ttd
LEFT JOIN city_council c ON ttd.District = c.District
WHERE c.District IS NULL;

-- Crime Trends Over Income Levels
SELECT 
    CASE 
        WHEN median_income < 30000 THEN 'Low Income (<30k)'
        WHEN median_income BETWEEN 30000 AND 60000 THEN 'Middle Income (30k-60k)'
        ELSE 'High Income (>60k)'
    END AS income_group,
    COUNT(id) AS total_crimes,
    AVG(crime_rate) AS avg_crime_rate
FROM int_acc
GROUP BY income_group
ORDER BY avg_crime_rate DESC;

-- Identifying High Crime & High Poverty Districts
SELECT 
    c.district_name, 
    AVG(i.crime_rate) AS avg_crime_rate, 
    AVG(i.poverty_rate) AS avg_poverty_rate
FROM int_acc i
JOIN tract_to_district ttd ON i.census_tract = ttd.TRACTCE10
JOIN city_council c ON ttd.District = c.District
GROUP BY c.district_name
ORDER BY avg_crime_rate DESC, avg_poverty_rate DESC
LIMIT 10;

-- Crime Rate and Internet Access Correlation
SELECT 
    district_name, 
    AVG(crime_rate) AS avg_crime_rate, 
    AVG(pct_no_internet) AS avg_pct_no_internet
FROM int_acc i
JOIN tract_to_district ttd ON i.census_tract = ttd.TRACTCE10
JOIN city_council c ON ttd.District = c.District
GROUP BY district_name
ORDER BY avg_pct_no_internet DESC;

-- Top 5 Most Crime-Prone Census Tracts
SELECT census_tract, crime_rate
FROM int_acc
ORDER BY crime_rate DESC
LIMIT 5;

-- Districts with Largest Population & Crime Ratio
SELECT 
    c.district_name, 
    SUM(i.population_2020) AS total_population, 
    COUNT(i.id) AS total_crimes, 
    (COUNT(i.id) * 1000.0) / SUM(i.population_2020) AS crimes_per_1000_residents
FROM int_acc i
JOIN tract_to_district ttd ON i.census_tract = ttd.TRACTCE10
JOIN city_council c ON ttd.District = c.District
GROUP BY c.district_name
ORDER BY crimes_per_1000_residents DESC;

-- Age-Related Query (Improved to Ensure Age > 0)
SELECT age, COUNT(*) AS count
FROM int_acc
WHERE age > 0
GROUP BY age
ORDER BY age ASC;
