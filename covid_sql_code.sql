-----Dataset source----
--: ourworldindata.org/covid-deaths
--: Data Dictionary: https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-codebook.csv


-------------------------Checking to see that both datasets were loaded properly------------------------
--Checking covid deaths dataset
select *
from 
	portfolio_project..CovidDeaths
order by 
	continent, location

--checking vaccinations dataset
select location
from 
	portfolio_project..vaccinations



-------------------------------------Data Analysis---------------------------------------



-------NOTES ABOUT DATA---------
--important thing to notice is that date is a date time formatted field
--all of the times are in YMD HMS (00:00:00 for the seconds which means there should be no problems filtering on the date field) 
--because filtering on the data would only pick up the observations that happen at (example 1/1/21 00:00:0000 it wouldnt pick up 1/1/21 03:00:0000)
--new deaths is a varchar variable so you must cast it as an int
--------------------------------


-----------------------------------------------------------------------------------Selecting columns to get a general sense of the data-----------------------------------------------------------------------------------
select 
	location,
    date,
    total_cases,
    new_cases,
    total_deaths, 
    population
from 
	portfolio_project..CovidDeaths
order by 1,2 --you case use 1,2 to order by the first 2 columns selected or specify them by name
-------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------Comparing Total cases vs Total deaths - percentage chance of dying by location----------------------------------------------------------------------------------------
select 
	location, 
	date, 
	total_cases, 
	total_deaths, 
	(total_deaths/total_cases)*100 as DeathPercentage
from 
	portfolio_project..CovidDeaths
order by 
	1,2
-------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------Comparing Total cases vs population- percentage of pop getting covid by location-----------------------------------------------------------
select 
	location, 
	date, 
	total_cases, 
	population, 
	(max(total_cases)/population)*100 as infectionpercentage
from 
	portfolio_project..CovidDeaths
order by 
	1,2
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------Comparing countries with highest infection rate vs population-----------------------------------------------------------
select 
	location,
	population,
	max(total_cases) as highestinfectedcount,  
	max((total_cases/population))*100 as infectionpercentage
from 
	portfolio_project..CovidDeaths
where 
	continent is not null     --this removes the individual continent totals from showing up
group by 
	location , population
order by 
	4
	--1 way to deal with null values is to replace them with averages from the rest of the data
-- for example replacing all countries with infection rates of NA with the avg of all the countries (done below)
-------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------Avg infection rate for entire world -----------------------------------------------------------
with infectionpercentagebycountry as (
select 
	location,
	population,
	max(total_cases) as highestinfectedcount,  
	max((total_cases/population))*100 as infectionpercentage
from 
	portfolio_project..CovidDeaths
where 
	continent is not null
group by 
	location , population
)
select 
	avg(infectionpercentage)
from 
	infectionpercentagebycountry


--Replacing null for infection rates for each country
drop view if exists replacingnullinfectionratespercountry

create view replacingnullinfectionratespercountry as
select 
	location,
	population,
	max(total_cases) as highestinfectedcount,  
	max((total_cases/population))*100 as infectionpercentage
from 
	portfolio_project..CovidDeaths
where 
	continent is not null     --this removes the individual continent totals from showing up
group by 
	location , population

--Using the ISNULL function to change the null locations to the world average
select
	location,
	isnull(infectionpercentage, 3.59394989503389)
from replacingnullinfectionratespercountry


-------------------------------------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------Comparing countries with highest death rate vs population-----------------------------------------------------------------------------
select 
	location,
	population,
	max(total_deaths) as total_deaths
from 
	portfolio_project..CovidDeaths
group by 
	location, population
order by 
	location DESC

--in the data there are instances where the location is the entire world or a specific continent (we dont want that) we want to view it when the continent is null
select 
	location,population,
	max(cast(total_deaths as int)) as totaldeaths
from 
	portfolio_project..CovidDeaths
where 
	continent is null 
group by 
	location , 
	population
order by 
	totaldeaths DESC
----------------------------------------------------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------Breaking things out by continent (removing the world records)-----------------------------------------------------------------------------

-----------------------------------------------------------------------------showing highest death count-----------------------------------------------------------------------------
select 
	location,
	max(cast(total_deaths as int)) as totaldeaths
from 
	portfolio_project..CovidDeaths
where 
	continent is null 
	and location not like 'World'
group by 
	location
order by 
	totaldeaths DESC




-----------------------------------------------------------------------------Showing the cases/death and death percentage world wide-----------------------------------------------------------------------------
select date, 
	SUM(new_cases) as total_cases,
	SUM(cast(new_deaths as int)) as total_deaths,
	(SUM(cast(new_deaths as int)))/(SUM(new_cases)) * 100 as death_percentage
from 
	portfolio_project..CovidDeaths
where 
	continent is null and 
	new_deaths != 0 --setting new deaths != 0 because we get an error when dividing by 0 deaths 
group by 
	date
order by 
	date ASC


-----------------------------------------------------------------------------Worldwide death % of total cases-----------------------------------------------------------------------------
select 
	SUM(new_cases) as total_cases, 
	SUM(cast(new_deaths as int)) as total_deaths,
	(SUM(cast(new_deaths as int)))/(SUM(new_cases)) * 100 as death_percentage
from 
	portfolio_project..CovidDeaths
where 
	continent is null and 
	new_deaths != 0 --seeting new deaths != 0 because we get an error when divinding by 0 deaths even if there were cases 
--group by date
order by 
	1,2


-----------------------------------------------------------------------------JOINING THE COVID DEATHS TABLE AND THE COVID VACCINATIONS TABLE-----------------------------------------------------------------------------


-----------------------------------------------------------------------------Looking at total population vs total vacinations by location and date-----------------------------------------------------------------------------
select 
	cd.location,date = cast(cd.date as date), 
	cd.population, 
	v.new_vaccinations,
	sum(convert(bigint, v.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as rolling_count_of_vaccs --using partition to sum all of the vaccinations by location on a per date basis
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and               --join on location as it is more specific than continent
	cd.date = v.date
where 
	cd.continent is null
order by 
	cd.location, cd.date


-----------------------------------------------------------------------------Using CTE to get the total percentage of people vacinated per day in each location-----------------------------------------------------------------------------
--since we are taking the population and dividing it by dates in which there will be 0 or NULL vaccination we must use the command (set ARITHABORT OFF) inorder to run the query and not error out
--this can be applied to the query from before as well

set ARITHABORT OFF

with popvsvacpercent (location, date, population, new_vaccinations, rolling_count_of_vaccs) as (
select 
	cd.location,
	date = cast(cd.date as date), --getting rid of HMS
	cd.population, 
	v.new_vaccinations,
	sum(convert(bigint, v.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as rolling_count_of_vaccs --converting to big int because of error string is too long
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and               --join on location as it is more specific than continent
	cd.date = v.date
where 
	cd.continent is null and cd.location not like '%world%')

select * , 
	percentageoftotalpopvaccinated = (rolling_count_of_vaccs/population) * 100
from 
	popvsvacpercent



-----------------------------------------------------------------------------looking at max percent vacc vs pop per location using the previous CTE-----------------------------------------------------------------------------
with popvsvacpercent (location, date, population, new_vaccinations, rolling_count_of_vaccs) as (
select 
	cd.location,date = cast(cd.date as date), 
	cd.population, 
	v.new_vaccinations,
	sum(convert(bigint, v.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as rolling_count_of_vaccs
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and               --join on location as it is more specific than continent
	cd.date = v.date
where 
	cd.continent is null and cd.location not like '%world%'
	)
select 
	location , 
	percentageoftotalpopvaccinated = max((rolling_count_of_vaccs/population) * 100)
from 
	popvsvacpercent
group by 
	location
order by 
	2

-----------------------------------------------------------------------------using a temp table instead (same as previous query)-----------------------------------------------------------------------------
Drop table if exists #percenatagepopulationvaccinated --Adding a drop table statement that way if changes are needed it is easy to rerun the selected query
--Creating table
create table #percenatagepopulationvaccinated
(
location nvarchar(255),
dates datetime,
population numeric,
new_vaccinations numeric,
rolling_count_of_vaccs numeric
)
--inserting records into temp table
insert into #percenatagepopulationvaccinated
select 
	cd.location, 
	dates = cast(cd.date as date), 
	cd.population, 
	v.new_vaccinations,
	sum(convert(bigint, v.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as rolling_count_of_vaccs
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and               --join on location and date as it is more specific than continent
	cd.date = v.date
where 
	cd.continent is null
--selecting from temp table
select 
	* , 
	(rolling_count_of_vaccs/population)*100
from 
	#percenatagepopulationvaccinated

-----------------------------------------------------------------------------Creating Views-----------------------------------------------------------------------------
--Rather than a temp table these can be used at a later time 
--using views for visualizations as well
Drop view if exists percenatagepopulationvaccinated

create view percenatagepopulationvaccinated as 
select 
	cd.location, 
	dates = cast(cd.date as date), 
	cd.population, 
	v.new_vaccinations,
	sum(convert(bigint, v.new_vaccinations)) OVER (partition by cd.location order by cd.location, cd.date) as rolling_count_of_vaccs
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and               --join on location and date as it is more specific than continent
	cd.date = v.date
where 
	cd.continent is null
--Selecting from view
select *
from 
	percenatagepopulationvaccinated


-----------------------------------------------------------------------------Finding the amount of days it took to get the first death after the first case reported-----------------------------------------------------------------------------
--Looking at both tables joined
select *
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	v.location = cd.location and v.date = cd.date
order by 
	3,4

----------------------------------------------Table for earliest case----------------------------------------------
Drop view if exists firstcasetime

create view firstcasetime as 
select 
	location, 
	min(date) as firstcasedate
from 
	portfolio_project..CovidDeaths
where 
	continent is null and 
	new_cases >= 1 and 
	location not like '%world%'
group by 
	location

----------------------------------------------Table for earliest death----------------------------------------------
Drop view if exists firstdeathtime
create view firstdeathtime as 
select location, min(date) as firstdeathdate
from portfolio_project..CovidDeaths
where continent is null and new_deaths >= 1 and location not like '%world%'
group by location

----------------------------------------------Table for earliest vaccine----------------------------------------------
Drop view if exists firstvaccine
create view firstvaccine as 
select location, min(date) as firstvaccinedate
from portfolio_project..vaccinations
where continent is null and new_vaccinations >= 1 and location not like '%world%'
group by location

-----------------------------combining tables-----------------------------------------
select 
	c.location ,
	firstcasedate = cast(c.firstcasedate as date), 
	firstdeathdate = cast(d.firstdeathdate as date), 
	Amt_of_days_between_firstcase_and_firstdeath = DATEDIFF(day, firstcasedate, firstdeathdate),
	firstvaccinedate = cast(v.firstvaccinedate as date),
	Amt_of_days_between_firstcase_and_firstvaccine = DATEDIFF(day, firstcasedate, firstvaccinedate)
from 
	firstcasetime as c
	join firstdeathtime as d
	on c.location = d.location
	join firstvaccine as v
	on v.location = c.location
order by 
	c.location



----------------------------------------------do countries with higher population densities get more cases/population----------------------------------------------
Drop view if exists avg_pop_density
--creating view for avgerage population density
create view avg_pop_density as 
select avg(population_density) as avg_pop_density
from portfolio_project..vaccinations
where continent is not null

---checking avgerage population density view output
select avg_pop_density
from avg_pop_density


Drop view if exists cases_as_percent_of_population
--Creating view for total cases as a % of population
create view cases_as_percent_of_population as 
select max(total_cases)/max(population) as cases_per_population
from portfolio_project..CovidDeaths
where continent is not null

--checking view output
select avg(cases_per_population)
from cases_as_percent_of_population


--From the query below it does not seem that countries with higher population densities are more likley to have more cases
--in fact it seems that a large majority of countries with less than average density had more than the average number of cases
with location_density as (
select 
	cd.location, 
	cases_as_percent_of_population = (max(cd.total_cases)/max(cd.population)) * 100, 
	population_density = max(v.population_density)
from 
	portfolio_project..CovidDeaths as cd
	join portfolio_project..vaccinations as v
on 
	cd.location = v.location and cd.date = v.date
where 
	cd.continent is not null
group by 
	cd.location)

select *,
	location_order = Case
		when cases_as_percent_of_population is null then 1
		else 0
		End, 
	density_categorization = case
		when population_density > 388 then 'above_avg' --using average from cases as % of pop view created before
		else 'below_avg'
		end, 
	cases_as_pop_percentage = case
		when cases_as_percent_of_population >= 0.0233724354503714 then 'above_avg' --using average from pop density view created before
		else 'below_avg'
		end
from 
	location_density
order by 
	location_order ASC, 
	cases_as_percent_of_population DESC, 
	population_density DESC




--------------------Looking at locations/countries with higher stingeny did this have a correlation to amount of cases/deaths (lower sting = less cases/deaths)------------------------------
--(higher stringency is better on a scale of 0 - 100)--

--Selecting top 10 countries with highest stringency index 
select top 10 
	cd.location, 
	stringency_index = max(v.stringency_index), 
	first_case_date = min(cd.date), 
	deaths_as_percent_of_pop = round(max(cd.total_deaths)/max(cd.population), 7)
from 
	portfolio_project..vaccinations as v
	join portfolio_project..CovidDeaths as cd
on 
	v.location = cd.location
	and v.date = cd.date
where 
	(cd.continent is not null and stringency_index is not null and cd.total_deaths is not null and cd.population is not null) 
	and new_cases >= 1
group by 
	cd.location 
order by 
	stringency_index DESC

--Selecting top 10 countries with lowest stringency index 
select top 10 
	cd.location, 
	stringency_index = max(v.stringency_index), 
	first_case_date = min(cd.date), 
	deaths_as_percent_of_pop = round(max(cd.total_deaths)/max(cd.population), 7)
from 
	portfolio_project..vaccinations as v
	join portfolio_project..CovidDeaths as cd
on 
	v.location = cd.location
	and v.date = cd.date
where 
	(cd.continent is not null and stringency_index is not null and cd.total_deaths is not null and cd.population is not null) 
	and new_cases >= 1
group by cd.location 
order by stringency_index ASC


--taking count of # of records with stringency over and under 50
select COUNT(*)
from portfolio_project..vaccinations
where stringency_index <= 50

select COUNT(*)
from portfolio_project..vaccinations
where stringency_index > 50

--Selecting countries with stringency over 50
Drop view if exists string_over_50

create view string_over_50 as 
select 
	cd.location, 
	stringency_index = max(v.stringency_index),
	death_percentage = max(cd.total_deaths)/max(cd.population)
from 
	portfolio_project..vaccinations as v
	join portfolio_project..CovidDeaths as cd
on 
	v.location = cd.location
	and v.date = cd.date
where 
	(cd.continent is not null and stringency_index is not null and cd.total_deaths is not null and cd.population is not null) 
	and v.stringency_index > 50
group by cd.location

--Selecting countries with stringency under 50
Drop view if exists string_under_50

create view string_under_50 as 
select 
	cd.location, 
	stringency_index = max(v.stringency_index), 
	death_percentage = max(cd.total_deaths)/max(cd.population)
from 
	portfolio_project..vaccinations as v
	join portfolio_project..CovidDeaths as cd
on 
	v.location = cd.location
	and v.date = cd.date
where 
	(cd.continent is not null and stringency_index is not null and cd.total_deaths is not null and cd.population is not null) 
	and v.stringency_index <= 50
group by cd.location


--Combining both views
select stringency_index = 'Over 50', 
	avg_stringency_index =avg(stringency_index), 
	avg_death_percentage = avg(death_percentage)
from string_under_50
union all
select stringency_index = 'Under 50' ,
	avg_stringency_index = avg(stringency_index), 
	avg_death_percentage = avg(death_percentage)
from string_over_50

--does stringency rating have a significant impact? -- Seems like it does


-------------------------------------------do the High cardiovasc death locations get more vaccs quicker/ tests quicker--------------------------------
--No it seems like major countries recieved the vaccine first even though cardiovasc is not as bad for these countires
select 
	cd.location ,
	cardiovasc_death_per_1million = max(cardiovasc_death_rate), 
	population = max(population), 
	dateoffirstvacc =min(cast(cd.date as date)),
case 
	when max(v.cardiovasc_death_rate) > (select avg(cardiovasc_death_rate) from portfolio_project..vaccinations) then 'above average'
	when max(v.cardiovasc_death_rate) <= (select avg(cardiovasc_death_rate) from portfolio_project..vaccinations) then 'below average'
	end as death_categorization
from 
	portfolio_project..vaccinations as v
	join portfolio_project..CovidDeaths as cd 
on v.location = cd.location and v.date = cd.date
where cd.continent is not null and new_vaccinations >= 1
group by cd.location
order by dateoffirstvacc