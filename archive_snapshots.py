import params
import json
import requests
from requests.auth import HTTPDigestAuth
import urllib.request

print("\nDownloading latest Atlas snapshot\n")
  
url = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + params.target_project_id +"/clusters/" + params.cluster_name + "/snapshots"
resp = requests.get(url=url, auth=HTTPDigestAuth(params.target_api_user, params.target_api_key))

if(resp.ok):

    # Get the snapshot data
    snapshots = json.loads(resp.content)
    print ("There are {0} snapshots".format(len(snapshots["results"])))
    
    # Pretty print the results
    # print(json.dumps(snapshots["results"], indent=4, sort_keys=True))

    # Grab the most recent snapshot, which is the first entry
    snapshot = snapshots["results"][0]
    
    # Pretty print the results
    # print(json.dumps(snapshot, indent=4, sort_keys=True))

    snapshot_id = snapshot["id"]

    # Create a Restore Job 
    print ("\nCreating a Restore Job for the most recent snapshot\n")
    url = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + params.target_project_id +"/clusters/" + params.cluster_name + "/restoreJobs"
    snapData =    {
     "delivery" : {
       "expirationHours" : "1",
       "maxDownloads" : "1",
       "methodName" : "HTTP"
     },
     "snapshotId" : snapshot_id
     }

    resp = requests.post(url=url, auth=HTTPDigestAuth(params.target_api_user, params.target_api_key), json=snapData)
  
    if (resp.ok):

      # Get the restore job
      resp = requests.get(url=url, auth=HTTPDigestAuth(params.target_api_user, params.target_api_key))
      if(resp.ok):

        # Get the job data
        jobs = json.loads(resp.content)
        #print(json.dumps(jobs, indent=4, sort_keys=True))

        # print ("There are {0} restore jobs".format(len(jobs["results"])))

        # Grab the most recent restore job, which is the first entry
        job = jobs["results"][0]
        #print(json.dumps(job, indent=4, sort_keys=True))

        download_url = job["delivery"]["url"]
        timestamp = job["timestamp"]["date"]
        filename = timestamp + ".tar.gz"
 
        print('Beginning file download ...\n')

        local_filename, headers = urllib.request.urlretrieve(download_url, filename) 

        print('Your snapshot file is: ' + local_filename )
      
    elif resp.status_code == 409:
      print(resp)

else:
  print("Error - status code: " + str(resp.status_code))

print("\n")
  
