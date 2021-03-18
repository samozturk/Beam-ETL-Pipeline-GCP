# Beam-Pipeline-pubsub

This simple program is for learning purposes. \
-First, publisher.py reads the csv data and publish to relevant "movies" topic. \
-Second, processor.py gets the rows via subscription "movies_subscription" then process it to filter only comedy movies. After that it publishes it to "comedy_movies" topic.\
-Lastly, subscriber.py gets filtered rows via "comedy_movies_subscription" and prints it into console.\

Therefore, you need create relevant topics and subscriptions.\
Topic 1: Movies\
Subscription: movies_subscription\

Topic 2: comedy_movies\
Subscription: comedy_movies_subscription\
