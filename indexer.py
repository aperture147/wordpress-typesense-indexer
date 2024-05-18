import pymysql
from configparser import ConfigParser
import typesense
from phpserialize import loads
import os
import json
import csv

config = ConfigParser()
config.read('config.ini')

wordpress_config = config['wordpress']
mysql_config = config['mysql']
typesense_config = config['typesense']

wordpress_host = wordpress_config['host']

db_conn = pymysql.connect(
    host=mysql_config['host'], port=int(mysql_config['port']),
    user=mysql_config['user'], passwd=mysql_config['password'], 
    db=mysql_config['db_name'],
    connect_timeout=30,
    autocommit=False
)
print('db connected')
typesense_client = typesense.Client({
  'nodes': [{
    'host': typesense_config['host'],  # For Typesense Cloud use xxx.a1.typesense.net
    'port': typesense_config['port'],       # For Typesense Cloud use 443
    'protocol': typesense_config['protocol']   # For Typesense Cloud use https
  }],
  'api_key': typesense_config['api_key'],
  'connection_timeout_seconds': 3600
})
print('typesense client created')
def get_new_posts(post_id_chunk: list):
    
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT t.term_id, tt.term_taxonomy_id, tt.parent, t.name, t.slug, tt.taxonomy
            FROM wp0e_terms AS t
            JOIN wp0e_term_taxonomy AS tt ON tt.term_id = t.term_id
        """)
        
        term_taxonomy_result = cur.fetchall()
        print('all terms fetched')
        term_taxonomy_dict = {
            term_id: taxonomy_id
            for term_id, taxonomy_id, *_ in term_taxonomy_result
        }
        taxonomy_dict = {
            taxonomy_id: (term_name, term_slug, taxonomy_name, term_taxonomy_dict.get(parent_id))
            for _, taxonomy_id, parent_id, term_name, term_slug, taxonomy_name in term_taxonomy_result
        }
        print('taxonomy adjacent list built')
        cur.execute("""
            SELECT object_id, term_taxonomy_id
            FROM wp0e_term_relationships
            WHERE object_id IN %s
        """, (post_id_chunk,))
        term_relationship_list = cur.fetchall()
        print('term relationship fetched')
        print(term_relationship_list)
        post_taxonomy_dict = {}
        for post_id, taxonomy_id in term_relationship_list:
            post_taxonomy = post_taxonomy_dict.setdefault(post_id, {})
            _, _, taxonomy_name, _ = taxonomy_dict[taxonomy_id]
            post_taxonomy.setdefault(taxonomy_name, []).append(taxonomy_id)
        print(post_taxonomy_dict)
        
        print('post taxonomy list built')
        cur.execute("""
            SELECT
                p.id, p.comment_count, u.user_nicename, p.post_content,
                p.post_date, p.post_excerpt, p.post_modified, p.post_title,
                p.post_type, p_thumb.guid, p_thumb_m.meta_value, p_category_m.meta_value, p.guid, p.post_name
            FROM wp0e_posts AS p
            LEFT JOIN wp0e_postmeta AS pm ON pm.post_id = p.ID AND pm.meta_key = '_thumbnail_id'
            LEFT JOIN wp0e_postmeta AS p_category_m ON p_category_m.post_id = p.ID AND p_category_m.meta_key = '_primary_term_category'
            LEFT JOIN wp0e_posts AS p_thumb ON CAST(pm.meta_value AS INTEGER) = p_thumb.ID
            LEFT JOIN wp0e_postmeta as p_thumb_m on p_thumb_m.post_id = p_thumb.ID AND p_thumb_m.meta_key = '_wp_attachment_metadata'
            LEFT JOIN wp0e_users as u on u.ID = p.post_author
            WHERE p.id IN %s
        """, (post_id_chunk,))
        print('post fetched')
        post_list = cur.fetchall()

    typesense_list = []
    for id, comment_count, post_author, \
        post_content, post_date, post_excerpt, \
        post_modified, post_title, post_type, \
        thumb_url, thumb_meta_str, category_id_str, post_link, post_name in post_list:
        print('processing post', id)
        current_post_taxonomy = post_taxonomy_dict[id]
        if category_id_str:
            category_id = int(category_id_str)
        else:
            category_id = int(current_post_taxonomy.get('category', [0])[0])


        
        category = []
        cat_link = []
        
        permalink = post_link

        cat_link_part = []
        if category_id:
            parent_id = category_id # init
            while parent_id:
                term_name, term_slug, _, parent_id = taxonomy_dict[parent_id]
                category.append(term_name)
                cat_link_part.append(term_slug)
            if cat_link_part:
                cat_link_part.reverse()
                cat_link.append(os.path.join(wordpress_host, 'category', *cat_link_part))
                permalink = os.path.join(wordpress_host, *cat_link_part, post_name, post_author)
        print('permalink', permalink)
        
        tag = []
        tag_link = []
        tag_tax_id_list = current_post_taxonomy.get('post_tag', [])
        for tax_id in tag_tax_id_list:
            term_name, term_slug, _, parent_id = taxonomy_dict[tax_id]
            tag.append(term_name)
            tag_link.append(os.path.join(wordpress_host, 'tag', term_slug))

        thumb_meta = loads(thumb_meta_str.encode(), decode_strings=True)
        thumb_html = f"<img width=\"{thumb_meta['width']}\" height=\"{thumb_meta['height']}\" src=\"{thumb_url}\" class=\"ais-Hit-itemImage\" alt=\"{post_title}\" decoding=\"async\" loading=\"lazy\" />"
        typesense_data = {
            "id": str(id),
            "comment_count": comment_count,
            "is_sticky": 0, # what is this?
            "permalink": permalink,
            "post_author": post_author, 
            "post_content": post_content,
            "post_date": str(post_date.date()),
            "post_excerpt": post_excerpt,
            "post_id": str(id),
            "post_modified": str(post_modified),
            "post_thumbnail": thumb_url,
            "post_thumbnail_html": thumb_html,
            "post_title": post_title,
            "post_type": post_type,
            "sort_by_date": int(post_date.timestamp()),
            "tag_links": tag_link,
            "tags": tag,
            "cat_link": cat_link,
            "category": category
        }
        typesense_list.append(typesense_data)
    with open('result.json', 'w') as f:
        json.dump(typesense_list, f)

    # typesense_client.collections['post'].documents.import_(typesense_list, {'action': 'upsert'})

def get_post_id():
    with open('data.csv') as csv_f:
        reader = csv.reader(csv_f)
        next(reader, None)
        return [*set(post_id for post_id, *_ in reader)]

def main():
    post_id_list = get_post_id()
    get_new_posts(post_id_list)

if __name__ == '__main__':
    try:
        main()
    finally:
        db_conn.close()