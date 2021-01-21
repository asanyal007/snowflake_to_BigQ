
class CreateLakes:
    def __init__(self, storage):
        self.storage = storage

    def create_deltalake(self, data, folder, filename, mode, partition_by): # only works with Spark 2.4
        if partition_by:
            data.write.partitionBy(partition_by).format("delta").mode(mode).save(self.storage+"/"+ folder + "/"+ filename)
        else:
            data.write.format("delta").mode(mode).save(self.storage + "/" + folder + "/" + filename)

