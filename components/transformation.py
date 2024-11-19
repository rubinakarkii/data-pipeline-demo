from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json
import os

DATABASE_NAME= os.getenv('database_name')
QUALYS_COLLECTION= os.getenv('qualys_collection')
CROWDSTRIKE_COLLECTION= os.getenv('crowdstrike_collection')

breakpoint()
spark = SparkSession.builder \
    .appName("DataPipeline") \
    .config(f'"spark.mongodb.output.uri", "mongodb://127.0.0.1/{DATABASE_NAME}"') \
    .getOrCreate()


with open("components/schema/common.json", 'r') as schema_file:
    schema_dict = json.load(schema_file)
    schema_common = StructType.fromJson(schema_dict)


def transform_qualys_data(api_data_qualys):
    transformed_df_qualys = spark.read.json(spark.sparkContext.parallelize(api_data_qualys), schema=schema_common)
    return transformed_df_qualys


def transform_crowdstrike_data(api_data_crowdstrike):
    transformed_df_crowdstrike = spark.read.json(spark.sparkContext.parallelize(api_data_crowdstrike), schema=schema_common)
    return transformed_df_crowdstrike

# api_data_crowdstrike = [{'_id': 'e64d9e3eb9f24f44818240d7f9ad4ebc', 'device_id': 'e64d9e3eb9f24f44818240d7f9ad4ebc', 'cid': '039b8503f1f44628b8fa96d590d96896', 'agent_load_flags': '0', 'agent_local_time': '2023-02-27T23:46:02.792Z', 'agent_version': '6.50.14713.0', 'bios_manufacturer': 'Xen', 'bios_version': '4.11.amazon', 'config_id_base': '65994763', 'config_id_build': '14713', 'config_id_platform': '8', 'cpu_signature': '198386', 'external_ip': '52.201.231.187', 'mac_address': '12-5e-2e-db-58-99', 'instance_id': 'i-086fc5a571e41b095', 'service_provider': 'AWS_EC2_V2', 'service_provider_account_id': '534968039550', 'hostname': 'ip-172-31-93-76.ec2.internal', 'first_seen': '2022-09-22T22:35:56Z', 'last_seen': '2023-03-16T13:34:47Z', 'local_ip': '172.31.93.76', 'major_version': '5', 'minor_version': '10', 'os_version': 'Amazon Linux 2', 'platform_id': '3', 'platform_name': 'Linux', 'policies': [{'policy_type': 'prevention', 'policy_id': '6be7b21acc6d4727a51b0566b2641c85', 'applied': True, 'settings_hash': 'd4cbb29', 'assigned_date': '2022-09-22T22:37:11.874047241Z', 'applied_date': '2022-09-22T22:37:44.906818295Z', 'rule_groups': []}], 'reduced_functionality_mode': 'no', 'device_policies': {'prevention': {'policy_type': 'prevention', 'policy_id': '6be7b21acc6d4727a51b0566b2641c85', 'applied': True, 'settings_hash': 'd4cbb29', 'assigned_date': '2022-09-22T22:37:11.874047241Z', 'applied_date': '2022-09-22T22:37:44.906818295Z', 'rule_groups': []}, 'sensor_update': {'policy_type': 'sensor-update', 'policy_id': 'ff994ba250164e3f9e91c090bc08c88c', 'applied': True, 'settings_hash': 'tagged|5;', 'assigned_date': '2023-02-27T23:43:59.293761339Z', 'applied_date': '2023-02-27T23:47:41.463776945Z', 'uninstall_protection': 'UNKNOWN'}, 'global_config': {'policy_type': 'globalconfig', 'policy_id': 'cc4747a0a00a4d19a91df8eb15230727', 'applied': True, 'settings_hash': '7b023c26', 'assigned_date': '2023-03-02T23:21:07.034869871Z', 'applied_date': '2023-03-02T23:23:33.391024246Z'}, 'remote_response': {'policy_type': 'remote-response', 'policy_id': '9957f814e0c44a57acb73677025d7dd8', 'applied': True, 'settings_hash': '17550b92', 'assigned_date': '2022-09-22T22:37:11.874019305Z', 'applied_date': '2022-09-22T22:37:44.938181904Z'}}, 'groups': [], 'group_hash': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'product_type_desc': 'Server', 'provision_status': 'NotProvisioned', 'serial_number': 'ec26e6cb-2f47-bf3a-41e3-e75f58b6fc2f', 'status': 'normal', 'system_manufacturer': 'Xen', 'system_product_name': 'HVM domU', 'tags': [], 'modified_timestamp': {'$date': '2023-03-16T13:34:51.000Z'}, 'meta': {'version': '19208', 'version_string': '2:7830577088'}, 'zone_group': 'us-east-1b', 'kernel_version': '5.10.130-118.517.amzn2.x86_64', 'chassis_type': '1', 'chassis_type_desc': 'Other', 'connection_ip': '172.31.93.76', 'default_gateway_ip': '172.31.80.1', 'connection_mac_address': '12-5e-2e-db-58-99'}]
api_data_qualys = [
    {
      "_id": 305003660,
      "account": {
        "list": [
          {
            "HostAssetAccount": {
              "username": "root"
            }
          }
        ]
      },
      "address": "172.31.19.223",
      "agentInfo": {
        "location": "Ashburn,Virginia United States",
        "locationGeoLatitude": "39.0469",
        "lastCheckedIn": {
          "$date": "2023-07-26T04:27:35.000Z"
        },
        "locationGeoLongtitude": "-77.4903",
        "agentVersion": "6.0.0.41",
        "manifestVersion": {
          "sca": "VULNSIGS-SCA-2.5.824.2-1",
          "vm": "VULNSIGS-VM-2.5.825.2-1"
        },
        "activatedModule": "AGENT_VM,AGENT_SCA,AGENT_PM",
        "activationKey": {
          "title": "Cloud agent key",
          "activationId": "79535c40-6b10-4fe8-bec6-6f828736b5bf"
        },
        "agentConfiguration": {
          "id": 1000,
          "name": "Initial Profile"
        },
        "status": "STATUS_ACTIVE",
        "chirpStatus": "Inventory Manifest Downloaded",
        "connectedFrom": "18.212.216.193",
        "agentId": "1ecc8a88-5912-4058-b731-94a04a48db11",
        "platform": "Linux"
      },
      "biosDescription": "Xen 4.2.amazon 08/24/2006",
      "cloudProvider": "AWS",
      "created": "2022-08-18T22:18:40Z",
      "dnsHostName": "ip-172-31-19-223.ec2.internal",
      "fqdn": "ip-172-31-19-223.ec2.internal",
      "id": 305003660,
      "isDockerHost": "false",
      "lastComplianceScan": "2023-07-25T01:48:34Z",
      "lastLoggedOnUser": "reboot",
      "lastSystemBoot": "2023-02-21T03:21:34Z",
      "lastVulnScan": {
        "$date": "2023-07-26T04:20:52.000Z"
      },
      "manufacturer": "Xen",
      "model": "HVM domU",
      "modified": "2023-07-26T04:27:35Z",
      "name": "ip-172-31-19-223.ec2.internal",
      "networkGuid": "6b48277c-0742-61c1-82bb-cac0f9c4094a",
      "networkInterface": {
        "list": [
          {
            "HostAssetInterface": {
              "interfaceName": "eth0",
              "macAddress": "0a:9a:0e:ba:3f:d9",
              "gatewayAddress": "172.31.16.1",
              "address": "172.31.19.223",
              "hostname": "ip-172-31-19-223.ec2.internal"
            }
          },
          {
            "HostAssetInterface": {
              "address": "18.212.216.193",
              "hostname": "ip-172-31-19-223.ec2.internal"
            }
          },
          {
            "HostAssetInterface": {
              "interfaceName": "eth0",
              "macAddress": "0a:9a:0e:ba:3f:d9",
              "gatewayAddress": "172.31.16.1",
              "address": "fe80:0:0:0:89a:eff:feba:3fd9",
              "hostname": "ip-172-31-19-223.ec2.internal"
            }
          }
        ]
      },
      "openPort": {
        "list": [
          {
            "HostAssetOpenPort": {
              "serviceName": "universal addresses to rpc program number mapper",
              "protocol": "UDP",
              "port": 645
            }
          },
          {
            "HostAssetOpenPort": {
              "serviceName": "provides the isc dhcp client daemon and dhclient-script",
              "protocol": "UDP",
              "port": 546
            }
          },
          {
            "HostAssetOpenPort": {
              "serviceName": "provides the isc dhcp client daemon and dhclient-script",
              "protocol": "UDP",
              "port": 68
            }
          },
          {
            "HostAssetOpenPort": {
              "serviceName": "an ntp client/server",
              "protocol": "UDP",
              "port": 323
            }
          },
        ]
      },
      "os": "Amazon Linux 2",
      "processor": {
        "list": [
          {
            "HostAssetProcessor": {
              "name": "Intel(R) Xeon(R) CPU E5-2676 v3 @ 2.40GHz",
              "speed": 2400
            }
          }
        ]
      },
      "qwebHostId": 215133728,
      "software": {
        "list": [
          {
            "HostAssetSoftware": {
              "name": "hunspell-en-US",
              "version": "0.20121024-6.amzn2.0.1.noarch"
            }
          }
        ]
      },
      "sourceInfo": {
        "list": [
          {
            "Ec2AssetSourceSimple": {
              "instanceType": "t2.micro",
              "subnetId": "subnet-01f8e12a21584302c",
              "imageId": "ami-024bc1da6bc3c5973",
              "groupName": "AutoScaling-Security-Group-1",
              "accountId": "534968039550",
              "macAddress": "0a:9a:0e:ba:3f:d9",
              "createdDate": "2022-08-02T00:16:58Z",
              "reservationId": "r-0630622542ee48001",
              "instanceId": "i-0f74daf3d7225083a",
              "monitoringEnabled": "false",
              "spotInstance": "false",
              "zone": "VPC",
              "instanceState": "RUNNING",
              "privateDnsName": "ip-172-31-19-223.ec2.internal",
              "vpcId": "vpc-092653ee3a9df275d",
              "type": "EC_2",
              "availabilityZone": "us-east-1c",
              "privateIpAddress": "172.31.19.223",
              "firstDiscovered": "2022-08-18T22:18:40Z",
              "ec2InstanceTags": {
                "tags": {
                  "list": []
                }
              },
              "publicIpAddress": "18.212.216.193",
              "lastUpdated": "2023-07-25T19:53:43Z",
              "region": "us-east-1",
              "assetId": 305003660,
              "groupId": "sg-071e536c5d05d82f8",
              "localHostname": "ip-172-31-19-223.ec2.internal",
              "publicDnsName": "ec2-18-212-216-193.compute-1.amazonaws.com"
            }
          },
          {
            "AssetSource": {}
          }
        ]
      },
      "tags": {
        "list": [
          {
            "TagSimple": {
              "id": 33643630,
              "name": "Cloud Agent"
            }
          }
        ]
      },
      "timezone": "UTC",
      "totalMemory": 966,
      "trackingMethod": "QAGENT",
      "type": "HOST",
      "volume": {
        "list": [
          {
            "HostAssetVolume": {
              "free": 496959488,
              "name": "/dev",
              "size": 496959488
            }
          },
          {
            "HostAssetVolume": {
              "free": {
                "$numberLong": "3584946176"
              },
              "name": "/",
              "size": {
                "$numberLong": "8577331200"
              }
            }
          }
        ]
      },
      "vuln": {
        "list": [
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616285"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354794
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616288"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354788
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616287"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354795
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616291"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354796
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616289"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354789
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616290"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354786
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11730616286"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-08T19:26:05Z",
              "qid": 354792
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11921569794"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-23T18:33:10Z",
              "qid": 354827
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11921569792"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-03-23T18:33:10Z",
              "qid": 354826
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11231265092"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-01-26T12:26:07Z",
              "qid": 354648
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "11231265098"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-01-26T12:26:07Z",
              "qid": 354656
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699410341"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 105213
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699410342"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 45500
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699333720"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 105328
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699410374"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 105301
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699333721"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 45423
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699410343"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 45382
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "10699410344"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-12-16T15:20:43Z",
              "qid": 45490
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12833258981"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-06-06T20:03:01Z",
              "qid": 355329
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12833258984"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-06-06T20:03:01Z",
              "qid": 355328
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12833258980"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-06-06T20:03:01Z",
              "qid": 355324
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12833258982"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-06-06T20:03:01Z",
              "qid": 355330
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12081348408"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-04-06T18:41:31Z",
              "qid": 354852
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12081348410"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-04-06T18:41:31Z",
              "qid": 354849
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12081348411"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-04-06T18:41:31Z",
              "qid": 354850
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12081348409"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-04-06T18:41:31Z",
              "qid": 354851
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8970942800"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-25T21:31:32Z",
              "qid": 354051
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843787"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 45531
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843815"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 105936
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843817"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 354016
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843837"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 115046
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843843"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 353993
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843847"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 354023
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "8892843848"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2022-08-19T03:34:42Z",
              "qid": 354007
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12407356016"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-05-02T20:28:37Z",
              "qid": 354903
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12421800367"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-05-03T19:00:36Z",
              "qid": 354917
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12421800368"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-05-03T19:00:36Z",
              "qid": 354918
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12421800365"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-05-03T19:00:36Z",
              "qid": 354907
            }
          },
          {
            "HostAssetVuln": {
              "hostInstanceVulnId": {
                "$numberLong": "12421800366"
              },
              "lastFound": "2023-07-26T04:20:52Z",
              "firstFound": "2023-05-03T19:00:36Z",
              "qid": 354910
            }
          }
        ]
      }
    }
  ]
transform_qualys_data = transform_qualys_data(api_data_qualys)
breakpoint()
transform_qualys_data.write \
    .format("mongo") \
    .mode("append") \
    .option("collection", QUALYS_COLLECTION) \
    .save()
# transform_crowdstrike_data(api_data_crowdstrike)

# # Function to convert snake_case to camelCase
# def to_camel_case(snake_str):
#     components = snake_str.split('_')
#     return components[0] + ''.join(x.title() for x in components[1:])

# # Recursive function to traverse and modify column names in nested dictionaries
# def modify_columns(record):
#     # Create a new record with camelCase keys, recursively handle nested dictionaries
#     new_record = {}
#     for key, value in record.items():
#         new_key = to_camel_case(key)  # Convert the key to camelCase
#         if isinstance(value, dict):
#             # If the value is a dictionary, apply the transformation recursively
#             new_record[new_key] = modify_columns(value)
#         elif isinstance(value, list):
#             # If the value is a list, recursively modify each element (if it's a dictionary)
#             new_record[new_key] = [modify_columns(item) if isinstance(item, dict) else item for item in value]
#         else:
#             # If it's not a dictionary or list, just keep the value
#             new_record[new_key] = value
#     return new_record

# # Apply the transformation to the RDD before creating the DataFrame
# modified_rdd = spark.sparkContext.parallelize(api_data_crowdstrike).map(modify_columns)

# # Define the schema (you can keep it as it is)
# df = spark.read.json(modified_rdd, schema=schema_common)

# # Show the DataFrame with modified column names
# df.show()
