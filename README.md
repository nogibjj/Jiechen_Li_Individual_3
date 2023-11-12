[![CI](https://github.com/nogibjj/Jiechen_Li_Mini_6_MySQL/actions/workflows/ci.yml/badge.svg)](https://github.com/nogibjj/Jiechen_Li_Mini_6_MySQL/actions/workflows/ci.yml)

## Jiechen_Li_Mini_6_MySQL

### Purpose

* Design a complex SQL query involving joins, aggregation, and sorting
* Provide an explanation for what the query is doing and the expected results

### Dataset

The dataset is sellcted from [DATA.GOV](https://catalog.data.gov/dataset/school-attendance-by-student-group-and-district-2021-2022/resource/d923f39c-c84c-4fa9-a252-c1f6b465bd55) in the United States.
The dataset appears to represent attendance data for various student groups across different districts in Connecticut for the academic years 2021-2022, 2020-2021, and 2019-2020.

### SQL Query

The goal is to compare the attendance rates of the "All Students" group in the 2021-2022 academic year against the rates in the 2020-2021 academic year for each district. We want to find districts where the attendance rate increased, remained stable (with a variation of less than 1%), or decreased.

1. **Create Tables**

```sql
CREATE DATABASE School_Attendance;
DEFAULT CHARACTER SET = 'utf8mb4';
USE School_Attendance;

CREATE TABLE
    School_Attendance_Table (
        District_code VARCHAR(255),
        District_name VARCHAR(255),
        Category VARCHAR(255),
        Student_group VARCHAR(255),
        `2021-2022_student_count_-_year_to_date` INT,
        `2021-2022_attendance_rate_-_year_to_date` FLOAT,
        `2020-2021_student_count` INT,
        `2020-2021_attendance_rate` FLOAT,
        `2019-2020_student_count` INT,
        `2019-2020_attendance_rate` FLOAT,
        Reporting_period DATE,
        Date_update DATE
    );

SET GLOBAL local_infile=1;
show tables;

LOAD DATA
    LOCAL INFILE '/Users/castnut/Desktop/706_Data_Engineering/mini_6/Jiechen_Li_Mini_6_External_Database/School_Attendance_by_Student_Group_and_District__2021-2022.csv' INTO
TABLE
    School_Attendance_Table FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

-- Creating the attendance_2021_2022 table
CREATE TABLE
    attendance_2021_2022 (
        District_code VARCHAR(255),
        District_name VARCHAR(255),
        Category VARCHAR(255),
        Student_group VARCHAR(255),
        Student_count INT,
        Attendance_rate FLOAT
    );

-- Inserting data into the attendance_2021_2022 table

INSERT INTO
    attendance_2021_2022 (
        District_code,
        District_name,
        Category,
        Student_group,
        Student_count,
        Attendance_rate
    )
SELECT
    District_code,
    District_name,
    Category,
    student_group,
    `2021-2022_student_count_-_year_to_date`,
    `2021-2022_attendance_rate_-_year_to_date`
FROM
    `School_Attendance_Table`;

-- Creating the attendance_2020_2021 table
CREATE TABLE
    attendance_2020_2021 (
        District_code VARCHAR(255),
        District_name VARCHAR(255),
        Category VARCHAR(255),
        Student_group VARCHAR(255),
        Student_count INT,
        Attendance_rate FLOAT
    );

-- Inserting data into the attendance_2020_2021 table
INSERT INTO
    attendance_2020_2021 (
        District_code,
        District_name,
        Category,
        Student_group,
        Student_count,
        Attendance_rate
    )
SELECT
    District_code,
    District_name,
    Category,
    student_group,
    `2020-2021_student_count`,
    `2020-2021_attendance_rate`
FROM
    `School_Attendance_Table`;

```

2. **School Attendance Rate Difference**

```sql
WITH Comparison AS (
        SELECT
            a21.District_code,
            a21.District_name,
            a21.Attendance_rate AS rate_2021_2022,
            a20.Attendance_rate AS rate_2020_2021, (
                a21.Attendance_rate - a20.Attendance_rate
            ) AS rate_difference
        FROM
            attendance_2021_2022 AS a21
            JOIN attendance_2020_2021 AS a20 ON a21.District_code = a20.District_code
        WHERE
            a21.Student_group = 'All Students'
            AND a20.Student_group = 'All Students'
    )
SELECT
    District_code,
    District_name,
    CASE
        WHEN rate_difference > 0.01 THEN 'Increased'
        WHEN rate_difference BETWEEN -0.01 AND 0.01 THEN 'Stable'
        ELSE 'Decreased'
    END AS Attendance_trend,
    rate_2021_2022,
    rate_2020_2021
FROM Comparison
ORDER BY rate_difference DESC;
```

### Explanation

In the WITH clause, I define a Common Table Expression (CTE) called Comparison to join the two tables (attendance_2021_2022 and attendance_2020_2021) based on the District code. This CTE filters the records for the "All Students" group and computes the difference in attendance rates between the two academic years.

In the main query, I use a CASE statement to categorize the attendance trend as 'Increased', 'Stable', or 'Decreased' based on the rate difference.

The final results are ordered by "rate_difference" in descending order, meaning districts with the most significant increase in attendance rate will be shown first.

### Results

The result will be a list of districts, their attendance rates for the "All Students" group in the 2021-2022 and 2020-2021 academic years. The visualization of top 15 districts based on rate difference in attendance is as following:

<img decoding="async" src="comparison_rates.png" width="85%">  

Please check ``sql_results_plot.py`` for details.

### Reference

Please click <a href="https://github.com/nogibjj/Jiechen_Li_Mini_2_Pandas.git" target="_blank">here</a> to see the template of this repo.
