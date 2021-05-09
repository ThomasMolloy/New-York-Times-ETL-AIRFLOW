------------------------- Spinning Docker Container -------------------------
$~ make run

------------------------- Project Structure -------------------------
The project is separated into two DAGS, nyt_best_sellers_ETL and nyt_best_sellers_email. 

The first dag is responsible for extracting data from the New York Times API, transforming the data (not much of a transformation necessary for this project however I found it necessary to include), and loading the data into a postgresql database. Originally, I began creating the etl similar to the small airflow project I did before the first interview which involved a simple bash operator however I didnt find it to be modular enough so I tried out the taskflow feature to separate each part of the pipeline as it seemed intuitive considering each next step relies on the previous. The dag runs daily, however in order to ensure the data is only extracted weekly, I created an airflow variable that kept track of the date of the last update and was used in the api call. Specifically, I raised an AirflowError in the extract task on the api call, so if it was successful than the update variable would be  incremented 7 days causing the dag to fail (with no retrys) until a week later when the api was updated (Im not sure if allowing dags/tasks to fail is bad practice, I hope not).

The second dag is responsible for sending the automated email. To make sure that email recipients did not receive the same list more than once, I used an external task sensor tied to the etl dag/load task, thus recipients would only recieve an email when best seller list was updated and loaded into the database.

------------------------- Areas to Improve -------------------------
-> As mentioned above, I spent far too much time trying to implement the mysql hook operator as I felt it was the cleanest way of obtaining data from the mysql database for the email only to run out of time to implement anything else.

-> I should be implementing far more error handling, especially around the api call.

-> Seems like its bad practice to edit airflow variables, especially with how I implemented it considering if the extract method succeeds, however transform or load fails, than the update variable will increment preventing the pipeline from succeeding on any retrys and guaranteeing missing a data point if its unmonitored.
