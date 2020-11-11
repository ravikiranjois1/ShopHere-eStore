#!/usr/bin/env python
# coding: utf-8

# In[37]:


import boto3


# In[38]:


photo = '/Users/shaivyachandra/Downloads/IMG_4242A.jpg'


# In[39]:


client = boto3.client('rekognition',aws_access_key_id = 'AKIAXL2Y2YVQ2SQQQQ3H',aws_secret_access_key='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')


# In[40]:


with open(photo,'rb') as source_image:
    source_bytes = source_image.read()


# In[41]:


response1 = client.detect_labels(
    Image={
        
        'Bytes': source_bytes,
        
        }
    ,
    MaxLabels=3,
    MinConfidence = 99
)


# In[42]:


print(response1)


# In[44]:


response2 = client.detect_labels(
    Image={
        
        'S3Object': {
            'Bucket':'cf-templates-y2yc9afuzxl7-us-east-1' ,
            'Name': 'IMG_3994.JPG'
            
        }
    }
    ,
    MaxLabels=3,
    MinConfidence = 99)
    


# In[45]:


print(response2)


# In[48]:


response3 = client.detect_labels(
    Image={
        
        'S3Object': {
            'Bucket':'screkognition' ,
            'Name': 'IMG_3985.JPG'
            
        }
    }
    ,
    MaxLabels=3,
    MinConfidence = 99)
    


# In[49]:


print(response3)


# In[ ]:




