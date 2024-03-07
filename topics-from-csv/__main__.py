import pulumi
import pulumi_confluentcloud as ccloud
from utils import read_excel_generate_topic_configs

# Pipeline to parse CSV
file_path = '/PATH/TO/EXCEL.xlsx' 
topic_configs = read_excel_generate_topic_configs(file_path)


i=1
topics=[]
for index, topic_data in enumerate(topic_configs):
    topic = ccloud.KafkaTopic(topic_name=topic_data["name"],
                            config=topic_data["config"],
                            partitions_count=topic_data["partitions_count"],
                            resource_name="topic_"+str(index))
    topics.append(topic)

topic.id.apply(lambda id: print('topic:', id))