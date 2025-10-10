from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import json

# ============ 配置区域 ============
# 替换为你的OpenSearch endpoint（不包含https://）
OPENSEARCH_HOST = 'search-restaurants-domain-l7g3jfsjjguyni3v5j3vblurkq.us-east-1.es.amazonaws.com'

REGION = 'us-east-1'
INDEX_NAME = 'restaurants'

# ============ AWS authentication ============
def get_aws_auth():
    """获取AWS认证"""
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        REGION,
        'es',
        session_token=credentials.token
    )
    return awsauth

# ============ connect to OpenSearch ============
def create_opensearch_client():
    """创建OpenSearch客户端"""
    awsauth = get_aws_auth()
    
    client = OpenSearch(
        hosts=[{'host': OPENSEARCH_HOST, 'port': 443}],
        http_auth=awsauth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        timeout=30
    )
    
    return client

# ============ 创建索引 ============
def create_index(client):
    """创建restaurants索引"""
    
    # 定义索引结构
    index_body = {
        'settings': {
            'index': {
                'number_of_shards': 1,
                'number_of_replicas': 0
            }
        },
        'mappings': {
            'properties': {
                'RestaurantID': {
                    'type': 'keyword'
                },
                'Cuisine': {
                    'type': 'keyword'
                }
            }
        }
    }
    
    # 检查索引是否存在
    if client.indices.exists(index=INDEX_NAME):
        print(f'Index "{INDEX_NAME}" already exists. Deleting...')
        client.indices.delete(index=INDEX_NAME)
    
    # 创建索引
    response = client.indices.create(index=INDEX_NAME, body=index_body)
    print(f'Index "{INDEX_NAME}" created successfully!')
    print(f'Response: {json.dumps(response, indent=2)}')

# ============ 从DynamoDB加载数据 ============
def load_data_from_dynamodb():
    """从DynamoDB读取所有餐厅数据"""
    
    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    table = dynamodb.Table('yelp-restaurants')
    
    print('Scanning DynamoDB table...')
    
    items = []
    response = table.scan()
    items.extend(response['Items'])
    
    # 处理分页
    while 'LastEvaluatedKey' in response:
        print(f'Fetching more items... (current count: {len(items)})')
        response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        items.extend(response['Items'])
    
    print(f'Total items fetched from DynamoDB: {len(items)}')
    return items

# ============ 插入数据到OpenSearch ============
def index_restaurants(client, restaurants):
    """将餐厅数据插入OpenSearch"""
    
    success_count = 0
    error_count = 0
    
    for i, restaurant in enumerate(restaurants):
        try:
            # 只存储RestaurantID和Cuisine
            doc = {
                'RestaurantID': restaurant['BusinessID'],
                'Cuisine': restaurant['Cuisine']
            }
            
            # 插入文档
            response = client.index(
                index=INDEX_NAME,
                body=doc,
                id=restaurant['BusinessID'],
                refresh=False  # 批量插入时设为False提高性能
            )
            
            success_count += 1
            
            if (i + 1) % 100 == 0:
                print(f'Indexed {i + 1} documents...')
                
        except Exception as e:
            error_count += 1
            print(f'Error indexing restaurant {restaurant.get("BusinessID")}: {str(e)}')
    
    # 刷新索引
    client.indices.refresh(index=INDEX_NAME)
    
    print(f'\n=== Indexing Complete ===')
    print(f'Successfully indexed: {success_count}')
    print(f'Errors: {error_count}')
    print(f'Total: {success_count + error_count}')

# ============ 验证数据 ============
def verify_data(client):
    """验证索引中的数据"""
    
    # 获取索引统计
    stats = client.indices.stats(index=INDEX_NAME)
    doc_count = stats['indices'][INDEX_NAME]['total']['docs']['count']
    print(f'\nTotal documents in index: {doc_count}')
    
    # 按cuisine统计
    query = {
        "size": 0,
        "aggs": {
            "cuisines": {
                "terms": {
                    "field": "Cuisine",
                    "size": 20
                }
            }
        }
    }
    
    response = client.search(index=INDEX_NAME, body=query)
    cuisines = response['aggregations']['cuisines']['buckets']
    
    print('\n=== Restaurants by Cuisine ===')
    for cuisine in cuisines:
        print(f"{cuisine['key']}: {cuisine['doc_count']} restaurants")
    
    # 测试搜索
    print('\n=== Testing Search (Chinese restaurants) ===')
    search_query = {
        "query": {
            "match": {
                "Cuisine": "Chinese"
            }
        },
        "size": 3
    }
    
    results = client.search(index=INDEX_NAME, body=search_query)
    for hit in results['hits']['hits']:
        print(f"RestaurantID: {hit['_source']['RestaurantID']}, Cuisine: {hit['_source']['Cuisine']}")

# ============ 主函数 ============
def main():
    print('=== Starting ElasticSearch Setup ===\n')
    
    try:
        # 1. 创建OpenSearch客户端
        print('Step 1: Connecting to OpenSearch...')
        client = create_opensearch_client()
        print('Connected successfully!\n')
        
        # 2. 创建索引
        print('Step 2: Creating index...')
        create_index(client)
        print()
        
        # 3. 从DynamoDB加载数据
        print('Step 3: Loading data from DynamoDB...')
        restaurants = load_data_from_dynamodb()
        print()
        
        # 4. 插入数据到OpenSearch
        print('Step 4: Indexing restaurants in OpenSearch...')
        index_restaurants(client, restaurants)
        print()
        
        # 5. 验证数据
        print('Step 5: Verifying indexed data...')
        verify_data(client)
        
        print('\n=== Setup Complete! ===')
        
    except Exception as e:
        print(f'\n!!! Error: {str(e)} !!!')
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()