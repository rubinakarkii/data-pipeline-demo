df.write \
    .format("mongo") \
    .mode("append") \
    .save()