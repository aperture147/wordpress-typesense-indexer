import pymysql
from configparser import ConfigParser
import typesense
import os
import math
from time import perf_counter, sleep, time
from datetime import datetime
import html
import argparse
import shutil

parser = argparse.ArgumentParser(
    prog='Typesense Wordpress Indexer',
    description='Index Posts from Wordpress'
)
parser.add_argument('--reindex', action='store_true',
                    help='reindex all posts')
parser.add_argument('--dryrun', action='store_true',
                    help='dry run')
args = parser.parse_args()
reindex = args.reindex
dryrun = args.dryrun

IDS_FILE = 'ids.txt'
CHECKPOINT_FILE = 'checkpoint.txt'
CHUNK_SIZE = 2000
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
    connect_timeout=120,
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
def index_new_posts(post_id_chunk: list):
    
    with db_conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT t.term_id, tt.term_taxonomy_id, tt.parent, t.name, t.slug, tt.taxonomy
            FROM wp_terms AS t
            JOIN wp_term_taxonomy AS tt ON tt.term_id = t.term_id
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
            FROM wp_term_relationships
            WHERE object_id IN %s
        """, (post_id_chunk,))
        term_relationship_list = cur.fetchall()
        print('term relationship fetched')
        post_taxonomy_dict = {}
        for post_id, taxonomy_id in term_relationship_list:
            post_taxonomy = post_taxonomy_dict.setdefault(post_id, {})
            _, _, taxonomy_name, _ = taxonomy_dict[taxonomy_id]
            post_taxonomy.setdefault(taxonomy_name, []).append(taxonomy_id)
        
        print('post taxonomy list built')
        cur.execute("""
            SELECT DISTINCT
                p.id, u.user_nicename, p.post_content,
                p.post_date, p.post_excerpt, p.post_modified,
                p.post_title, p.post_type, p_thumb.guid,
                p_meta.sku, p_meta.rating_count, p_price.meta_value
            FROM wp_posts AS p
            LEFT JOIN wp_posts AS p_thumb ON p_thumb.post_parent = p.ID AND p_thumb.post_type = 'attachment' AND p_thumb.post_mime_type LIKE %s
            LEFT JOIN wp_postmeta AS p_price ON p_price.post_id = p.ID AND p_price.meta_key = '_price'
            LEFT JOIN wp_users AS u ON u.ID = p.post_author
            LEFT JOIN wp_wc_product_meta_lookup AS p_meta ON p_meta.product_id = p.id
            WHERE p.id IN %s LIMIT %s
        """, ('image/%', post_id_chunk, len(post_id_chunk)))
        print('post fetched')
        post_list = cur.fetchall()

    typesense_list = []
    for id, post_author, post_content, \
        post_date, post_excerpt, post_modified, \
        post_title, post_type, thumb_url, \
        product_sku, rating_count, price_str in post_list:
        print('processing post', id)
        current_post_taxonomy = post_taxonomy_dict.get(id)
        price = float(price_str)
        category_id_list = []
        if current_post_taxonomy:
            category_id_list = [int(x) for x in current_post_taxonomy.get('product_cat', [])]

        category_list = []
        cat_link_list = []
        cat_link_html_list = []
        
        permalink = wordpress_host + f"?p={id}"

        if category_id_list:
            for category_id in category_id_list:
                cat_dict = {}
                cat_link_part = []
            
                parent_id = category_id # init
                while parent_id:
                    if parent_id not in taxonomy_dict:
                        break
                    term_name, term_slug, _, parent_id = taxonomy_dict[parent_id]
                    category_list.append(term_name)
                    cat_link_part.append(term_slug)
                    cat_dict[term_slug] = term_name
                if cat_link_part:
                    cat_link_part.reverse()
                    cat_link = os.path.join(wordpress_host, '3dsmax-download', *cat_link_part)
                    cat_link_html = f"<span class=\"posted_in\"><a href=\"{cat_link}\" rel=\"tag\">{cat_dict[cat_link_part[-1]]}</a>"
                    cat_link_list.append(cat_link)
                    cat_link_html_list.append(cat_link_html)
        
        tag_list = []
        tag_link_list = []
        tag_tax_id_list = current_post_taxonomy.get('post_tag', [])
        print('total categories:', len(category_list))
        print('total tags:', len(tag_list))
        for tax_id in tag_tax_id_list:
            term_name, term_slug, _, parent_id = taxonomy_dict[tax_id]
            tag_list.append(term_name)
            tag_link_list.append(os.path.join(wordpress_host, 'tag', term_slug))
        
        thumb_html = f"<img width=\"350\" height=\"350\" src=\"{thumb_url}\" class=\"attachment-woocommerce_thumbnail size-woocommerce_thumbnail\" alt=\"{html.escape(post_title)}\" 0=\"tsfwc-thumbnail_image\" decoding=\"async\" loading=\"lazy\" />"
        add_to_cart_btn = f"<a href=\"?add-to-cart={id}\" data-quantity=\"1\" class=\"button product_type_simple add_to_cart_button ajax_add_to_cart\" data-product_id=\"{id}\" data-product_sku=\"{product_sku}\" aria-label=\"Add to cart: &ldquo;{html.escape(post_title)}&rdquo;\" aria-describedby=\"\" rel=\"nofollow\">Add to cart</a>"
        price_html = f"<span class=\"woocommerce-Price-amount amount\"><bdi><span class=\"woocommerce-Price-currencySymbol\">&#36;</span>{'%.2f' % price}</bdi></span>"
        if not isinstance(post_date, datetime):
            post_date = datetime.now()
            print('malformed post date, set to current time')
        if not isinstance(post_modified, datetime):
            post_modified = datetime.now()
            print('malformed post modified, set to current time')
        print('permalink', permalink)
        id_str = str(id)
        typesense_data = {
            "add_to_cart_btn": add_to_cart_btn,
            "id": id_str,
            "is_sticky": 0, 
            "is_featured": 0,
            "permalink": permalink,
            "post_author": post_author or '', 
            "post_content": post_content,
            "post_date": str(post_date),
            "post_excerpt": post_excerpt,
            "post_id": id_str,
            "post_modified": str(post_modified),
            "post_thumbnail": thumb_url,
            "post_thumbnail_html": thumb_html,
            "post_title": post_title,
            "post_type": post_type,
            "price": price,
            "prices": [price],
            "price_html": price_html,
            "rating": rating_count,
            "sort_by_date": int(post_date.timestamp()),
            "tag_links": tag_link_list,
            "tags": tag_list,
            "cat_link": cat_link_list,
            "cat_links_html": ", ".join(cat_link_html_list),
            "category": category_list,
            "total_sales": 0 # TODO
        }
        typesense_list.append(typesense_data)
    if dryrun:
        import json
        with open('demo.json', 'w') as f:
            json.dump(typesense_list, f)
        return
    print('pushing to typesense')
    result = typesense_client.collections['product'].documents.import_(typesense_list, {'action': 'upsert'})
    if any(not x['success'] for x in result):
        print(result)
        raise Exception('failed to index to typesense')

def get_post_id():
    with open(IDS_FILE) as id_f:
        return [post_id.strip() for post_id in id_f.readlines() if post_id]

def read_checkpoint():
    if not os.path.isfile(CHECKPOINT_FILE):
        return 0
    try:
        with open(CHECKPOINT_FILE) as ckpt_f:
            last_chunk = ckpt_f.read().strip()
        return int(last_chunk)
    except ValueError as e:
        print('malformed checkpoint, reset to 0')
        return 0

def write_checkpoint(last_chunk):
    with open(CHECKPOINT_FILE, 'w') as ckpt_f:
        ckpt_f.write(str(last_chunk))

def backup_id_and_checkpoint():
    now = int(time())
    if os.path.isfile(IDS_FILE):
        shutil.copy(IDS_FILE, f'ids-backup-{now}.txt')
        os.remove(IDS_FILE)
    if os.path.isfile(CHECKPOINT_FILE):
        shutil.copy(CHECKPOINT_FILE, f'checkpoint-backup-{now}.txt')
        os.remove(CHECKPOINT_FILE)

def get_all_posts_from_db():
    
    with db_conn.cursor() as cur:
        cur.execute('''
            SELECT id FROM wp_posts
            WHERE post_status = 'publish' AND post_type = 'product'
        ''')
        result = cur.fetchall()
    backup_id_and_checkpoint()
    # write file for checkpoint
    with open(IDS_FILE, 'a') as f:
        for id, in result[:-1]:
            f.write(f'{id}\n')
        f.write(f'{result[-1][0]}')
    return [x for x, in result]

def main():
    start_time = time()
    if reindex:
        post_id_list = get_all_posts_from_db()
    else:
        post_id_list = get_post_id()
    print('total posts', len(post_id_list))
    chunk_count = math.ceil(len(post_id_list) / CHUNK_SIZE)
    print('total chunk', chunk_count)
    last_chunk = read_checkpoint()
    for i in range(last_chunk, chunk_count):
        chunk = post_id_list[i * CHUNK_SIZE: (i+1) * CHUNK_SIZE]
        print('processing chunk', i)
        start = perf_counter()
        index_new_posts(chunk)
        end = perf_counter()
        print('finished chunk', i, 'elapsed time', end-start, 'seconds, writing checkpoint')
        write_checkpoint(i)
        print('sleep 0.5 second')
        sleep(0.5)
    end_time = time()
    if os.path.isfile(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
    print('total elapsed time', end_time - start_time, 'seconds')

if __name__ == '__main__':
    try:
        main()
    finally:
        db_conn.close()