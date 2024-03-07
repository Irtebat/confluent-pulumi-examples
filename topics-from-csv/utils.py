import pandas as pd
import json

def read_excel_generate_topic_configs(file_path, sheet_name='Sheet1'):
    # Read the Excel file
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Initialize an empty list to store topic configurations
    topic_configs = []
    
    # Iterate through each row in the DataFrame to build the topic configuration
    for _, row in df.iterrows():
        # Build the topic configuration map
        topic_config = {
            "name": row['topic name'] if 'topic name' in row and pd.notna(row['topic name']) else "default-topic",
            "partitions_count": str(int(row['partitions'])) if 'partitions' in row and pd.notna(row['partitions']) else "1",
            "config": {
                "cleanup.policy": row['cleanup.policy'] if 'cleanup.policy' in row and pd.notna(row['cleanup.policy']) else "delete",
                "delete.retention.ms": str(int(row['delete.retention.ms'])) if 'delete.retention.ms' in row and pd.notna(row['delete.retention.ms']) else "86400000",
                "max.compaction.lag.ms": str(int(row['max.compaction.lag.ms'])) if 'max.compaction.lag.ms' in row and pd.notna(row['max.compaction.lag.ms']) else "9223372036854775807",
                "max.message.bytes": str(int(row['max.message.bytes'])) if 'max.message.bytes' in row and pd.notna(row['max.message.bytes']) else "2097164",
                "message.timestamp.after.max.ms": str(int(row['message.timestamp.after.max.ms'])) if 'message.timestamp.after.max.ms' in row and pd.notna(row['message.timestamp.after.max.ms']) else "9223372036854775807",
                "message.timestamp.before.max.ms": str(int(row['message.timestamp.before.max.ms'])) if 'message.timestamp.before.max.ms' in row and pd.notna(row['message.timestamp.before.max.ms']) else "9223372036854775807",
                "message.timestamp.difference.max.ms": str(int(row['message.timestamp.difference.max.ms'])) if 'message.timestamp.difference.max.ms' in row and pd.notna(row['message.timestamp.difference.max.ms']) else "9223372036854775807",
                "message.timestamp.type": row['message.timestamp.type'] if 'message.timestamp.type' in row and pd.notna(row['message.timestamp.type']) else "CreateTime",
                "min.compaction.lag.ms": str(int(row['min.compaction.lag.ms'])) if 'min.compaction.lag.ms' in row and pd.notna(row['min.compaction.lag.ms']) else "0",
                "min.insync.replicas": str(int(row[' min.insync.replicas'].strip())) if ' min.insync.replicas' in row and pd.notna(row[' min.insync.replicas']) else "2",
                "retention.bytes": str(int(row['retention.bytes'])) if 'retention.bytes' in row and pd.notna(row['retention.bytes']) else "-1",
                "retention.ms": str(int(row['retention.ms'])) if 'retention.ms' in row and pd.notna(row['retention.ms']) else "604800000",
                "segment.bytes": str(int(row['segment.bytes'])) if 'segment.bytes' in row and pd.notna(row['segment.bytes']) else "104857600",
                "segment.ms": str(int(row[' segment.ms'].strip())) if ' segment.ms' in row and pd.notna(row[' segment.ms']) else "604800000"
            }
        }
        
        # Append the configuration to the list
        topic_configs.append(topic_config)
    
    # Return the array of maps
    return topic_configs