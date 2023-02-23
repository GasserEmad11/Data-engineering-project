### Objective:
This file describes all the columns in the fact and dimensions tables

* Immigrant_fact table:

|NAME             |      DESCRIPTION                      |
|-----------------|---------------------------------------|
|id               |citizen id                             |
|visa_id          |the citizen visa issuance number       | 
|gender           |the gender of the citizen              |
|addmission number|the number represented when admitted   |
|airport id       |the code of the ariport of entry       |
|state code       |the postal code of the citizen's state |
|arrdate          |the citizen's arrival date into the usa|
|City             |the citizen's residency city           |
|i94visa          |the tier of visa                       |
|Race             |the majority race of the city          | 




* VISA_DIM:

|NAME            |   DESCRIPTION                       |
|----------------|-------------------------------------|
|visa id         |the citizen visa issuance number     |
|visa type       |the type of visa issued to individual|
|i94visa         |the tier of visa issued              |
|visa post       |the place where the visa was issued  |


* TEMP_DIM:

|NAME               |   DESCRIPTION                        |
|----------------   |--------------------------------------|
|date               |latest recorded temperature date      |
|average temperature| the average temperature of the city  |
|Country            |The country where the data is recorded|
|City               |The city where the data is recorded   |
|state_code         |the postal code of the city           | 

* DEMO_DIM:

|NAME            |   DESCRIPTION                           |
|----------------|-----------------------------------------|
|City            |The city where the data is recorded      |
|state           |The state where the city resides         |
|average_age     |the average age of city's residents      |
|population      |the population of the city               |
|foriegn_born    |the number of people born outside of us  |
|Race            |the majority race of the city            |
|state_code      |the state postal code                    |
|race_count      |the count of people of color in the state|



* AIRPORT_DIM:

|NAME            |   DESCRIPTION                           |
|----------------|-----------------------------------------|
|airport_id      |the code of airport                      |
|airport_type    |the type of intended airport             |
|airport_name    |the name of the airport                  |
|region          |the state where the airport exists       |




* TIME_DIM:

|NAME            |   DESCRIPTION                           |
|----------------|-----------------------------------------|
|arrdate         |the arrival date of the citizen to the us|
|date            |the fully formated date                  |
|year            |the year of arrival                      |
|month           |the month of arrival                     |
|day             |the day of arrival                       |

