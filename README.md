# startup
1. Set up environment in Docker:

  - docker-compose build
  - docker-compose up airflow-init
  - docker-compose up -d

2. Add connection in Airflow:

![image](https://user-images.githubusercontent.com/25270608/173841677-c6f4f974-c65b-4016-8ff0-cbdd3a32d897.png)
  - Connection Id: open_weather
  - Connection Type: HTTP
  - Host: api.openweathermap.org/

3. Turn on the toggle: ![image](https://user-images.githubusercontent.com/25270608/173842149-d90e7457-d12c-4d5a-8bc7-a64dfb570733.png)

And Voila
