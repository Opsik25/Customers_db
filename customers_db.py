import configparser
import psycopg2


def db_settings_parser():
    config = configparser.ConfigParser()
    config.read('settings.ini')
    db_settings = {}
    database = config['DB_user_password_data']['database']
    db_settings['database'] = database
    user = config['DB_user_password_data']['user']
    db_settings['user'] = user
    password = config['DB_user_password_data']['password']
    db_settings['password'] = password
    return db_settings


def create_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
        DROP TABLE IF EXISTS phones;
        DROP TABLE IF EXISTS customers;
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS customers(
            PRIMARY KEY (customer_id),
            customer_id  SERIAL       NOT NULL,
            name         VARCHAR(40)  NOT NULL,
            surname      VARCHAR(100) NOT NULL,
            email        VARCHAR(255) UNIQUE NOT NULL
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS phones(
            PRIMARY KEY (phone_number),
            phone_number VARCHAR(11) NOT NULL,
            customer_id  INTEGER     NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE ON UPDATE CASCADE
        );
        """)

        conn.commit()


def add_customer(conn, name: str, surname: str, email: str, phone_number=None):  # phone_number as a list
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO customers (name, surname, email)
        VALUES (%s, %s, %s); 
        """, (name, surname, email))

        if phone_number is not None:
            cur.execute("""
            SELECT customer_id
              FROM customers
             WHERE email=%s;
            """, (email,))
            customer_id = cur.fetchone()[0]

            for phone in phone_number:
                cur.execute("""
                INSERT INTO phones (phone_number, customer_id)
                VALUES (%s, %s);
                """, (phone, customer_id))

        cur.execute("""
        SELECT *
          FROM customers
          JOIN phones
            ON customers.customer_id = phones.customer_id
         WHERE phones.customer_id = %s;
        """, (customer_id,))
        print(cur.fetchall())


def add_phone_customer(conn, customer_id: int, phone_number: str):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO phones (phone_number, customer_id)
        VALUES (%s, %s);
        """, (phone_number, customer_id))

        cur.execute("""
        SELECT *
          FROM customers
          JOIN phones
            ON customers.customer_id = phones.customer_id
         WHERE phones.customer_id = %s;
        """, (customer_id,))
        print(cur.fetchall())


def change_customer_info(conn, customer_id: int, name=None, surname=None, email=None,
                         old_phone_number=None, new_phone_number=None):
    with conn.cursor() as cur:
        if name is not None:
            cur.execute("""
            UPDATE customers
               SET name = %s
             WHERE customer_id = %s;
            """, (name, customer_id))

        if surname is not None:
            cur.execute("""
            UPDATE customers
               SET surname = %s
             WHERE customer_id = %s;
            """, (surname, customer_id))

        if email is not None:
            cur.execute("""
            UPDATE customers
               SET email = %s
             WHERE customer_id = %s;
            """, (email, customer_id))

        if new_phone_number is not None:
            cur.execute("""
            UPDATE phones
               SET phone_number = %s
             WHERE customer_id = %s
               AND phone_number = %s;
            """, (new_phone_number, customer_id, old_phone_number))

        cur.execute("""
        SELECT *
          FROM customers
          JOIN phones
            ON customers.customer_id = phones.customer_id
         WHERE phones.customer_id = %s;
        """, (customer_id,))
        print(cur.fetchall())


def delete_phone(conn, customer_id: int, phone_number: str):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE
          FROM phones
         WHERE customer_id = %s
           AND phone_number = %s;
        """, (customer_id, phone_number))

        cur.execute("""
        SELECT *
          FROM customers
          JOIN phones
            ON customers.customer_id = phones.customer_id
         WHERE phones.customer_id = %s;
        """, (customer_id,))
        print(cur.fetchall())


def delete_customer(conn, customer_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        DELETE
          FROM customers 
         WHERE customer_id = %s;
        """, (customer_id,))

        conn.commit()
    print(f'Клиент с id {customer_id} удален из базы данных')


def search_customer(conn, name=None, surname=None, email=None, phone_number=None):
    with conn.cursor() as cur:
        if phone_number is not None:
            cur.execute("""
            SELECT customer_id
              FROM phones
             WHERE phone_number = %s;
            """, (phone_number,))
            customer_id = cur.fetchone()[0]

            cur.execute("""
            SELECT *
              FROM customers
              JOIN phones
                ON customers.customer_id = phones.customer_id
             WHERE phones.customer_id = %s;
            """, (customer_id,))
            print(cur.fetchall())
        else:
            cur.execute("""
            SELECT *
              FROM customers
              JOIN phones
                ON customers.customer_id = phones.customer_id
             WHERE name = %s
                OR surname = %s
                OR email = %s;
            """, (name, surname, email))
            print(cur.fetchall())


if __name__ == '__main__':
    db_enter_data = db_settings_parser()
    with psycopg2.connect(database=db_enter_data['database'], user=db_enter_data['user'],
                          password=db_enter_data['password']) as conn:
        create_tables(conn)
        add_customer(conn, 'Виталий', 'Иванов', 'vitaliyivanov@mail.ru', ['89111111111'])
        add_customer(conn, 'Дмитрий', 'Попов', 'dmitriypopov@mail.ru', ['89774526485'])
        add_customer(conn, 'Евгений', 'Попов', 'evgeniypopov@mail.ru', ['89036986542'])
        add_customer(conn, 'Дмитрий', 'Сидоров', 'dmitriysidorov@yandex.ru',
                     phone_number=['89000000000', '89999999999'])
        add_phone_customer(conn, 1, '89222222222')
        change_customer_info(conn, customer_id=1, name='Степан', surname='Петров',
                             old_phone_number='89111111111', new_phone_number='89333333333')
        delete_phone(conn, 1, '89222222222')
        delete_customer(conn, 1)
        search_customer(conn, name='Дмитрий')
        search_customer(conn, surname='Попов')
        search_customer(conn, email='dmitriysidorov@yandex.ru')
        search_customer(conn, phone_number='89999999999')
        search_customer(conn, phone_number='89036986542')
    conn.close()
