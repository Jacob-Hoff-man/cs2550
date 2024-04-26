import pickle
import random
import sys

from CatalogManager import CatalogManager
from Common import Component
from DataManager import DataManager
from Logger import LogType
from Scheduler import Scheduler
from TransactionManager import TransactionManager

coffee_list = [
    "After Dinner Blend Coffee",
    "All-American Apple Pie Flavored Coffee",
    "Almond Joy Flavored Coffee",
    "Amaretto Flavored Coffee",
    "American Spirit Coffees",
    "Apple Cider Donut Flavored Coffee",
    "Apple Strudel Flavored Coffee",
    "Apricot Cream Flavored Coffee",
    "Atlantic Blend Coffee",
    "Baked Apple Dumplin' Flavored Coffee",
    "Banana Cream Pie Flavored Coffee",
    "Banana Nut Bread Flavored Coffee",
    "Bananas Foster Flavored Coffee",
    "Big City Blend Coffee",
    "Black Forest Cake Flavored Coffee",
    "Black Satin Blend Coffee",
    "Blackberry Brandy Flavored Coffee",
    "Blackberry Cream Flavored Coffee",
    "Blackberry Merlot Flavored Coffee",
    "Blue Ridge Mountain Blend Coffee",
    "Blueberry Cinnamon Crumble Flavored Coffee",
    "Blueberry Cream Flavored Coffee",
    "Bob's Page",
    "Bourbon Whiskey Flavored Coffee",
    "Brazilian Nut Crunch Flavored Coffee",
    "Brazilian Santos Coffee",
    "Breakfast Blend Coffee",
    "Breakfast in a Cup Flavored Coffee",
    "Butter Nut Flavored Coffee",
    "Butter Pecan Flavored Coffee",
    "Butterscotch Flavored Coffee",
    "Cafe Femenino Fair Trade Organic Brazilian Coffee",
    "Cafe Femenino Fair Trade Organic Dominican Republic Coffee",
    "Cafe Mocha Flavored Coffee",
    "Cafe Reserva Coffee",
    "California Gold Flavored Coffee",
    "Candy Corn Flavored Coffee",
    "Caramel Candy Apple Flavored Coffee",
    "Caramel Flavored Coffee",
    "Caramel Nut Fudge Flavored Coffee",
    "Caramel Smooches Flavored Coffee",
    "Celebes Kalossi Coffee",
    "Cherry Cheesecake Flavored Coffee",
    "Cherry Vanilla Flavored Coffee",
    "Chocolate Almond Flavored Coffee",
    "Chocolate Cappuccino Flavored Coffee",
    "Chocolate Caramel Flavored Coffee",
    "Chocolate Cherry Kiss Flavored Coffee",
    "Chocolate Cinnamon Hazelnut Flavored Coffee",
    "Chocolate Fudge Flavored Coffee",
    "Chocolate Hazelnut Flavored Coffee",
    "Chocolate Macadamia Nut Flavored Coffee",
    "Chocolate Malt Flavored Coffee",
    "Chocolate Marshmallow Flavored Coffee",
    "Chocolate Mint Flavored Coffee",
    "Chocolate Nightmare Flavored Coffee",
    "Chocolate Orange Flavored Coffee",
    "Chocolate Peanut Butter Cup Flavored Coffee",
    "Chocolate Pretzel Stick Flavored Coffee",
    "Chocolate Raspberry Flavored Coffee",
    "Chocolate-Covered Easter Egg Flavored Coffee",
    "Christmas Blend Flavored Coffee",
    "Chunky Monkey Flavored Coffee",
    "Cinnabun Flavored Coffee",
    "Cinnamon Almond Macaroon Flavored Coffee",
    "Cinnamon Hazelnut Flavored Coffee",
    "Cinnamon Stick Flavored Coffee",
    "Classic Salted Caramel Flavored Coffee",
    "Cocoa Mocha Twist Flavored Coffee",
    "Coconut Cream Flavored Coffee",
    "Coconut Fudge Flavored Coffee",
    "Coffee Cake Flavored Coffee",
    "Coffee List A to Z",
    "Colombian Decaf Coffee",
    "Colombian Supremo Coffee",
    "Cookies and Cream Flavored Coffee",
    "Costa Rican Tarrazu Coffee",
    "Cranberry Cream Flavored Coffee",
    "Cranberry Nut Cream Flavored Coffee",
    "Creme Brulee Flavored Coffee",
    "Dark Chocolate Ecstasy Flavored Coffee",
    "Dark Roasted Coffee",
    "Decaf Coffee",
    "Dulce de Leche Flavored Coffee",
    "Dutch Chocolate Flavored Coffee",
    "Emerald Isle Delight Flavored Coffee",
    "English Toffee Flavored Coffee",
    "Espresso Cafe Milano #5 Blend Coffee",
    "Espresso Capri Blend Coffee",
    "Espresso Coffee",
    "Espresso Mugello Blend Decaf Coffee",
    "Espresso Regalo Blend Coffee",
    "Espresso Rocky Mountain Blend Coffee",
    "Ethiopian Harrar Coffee",
    "Fair Trade Organic Big Easy Blend Coffee",
    "Fair Trade Organic Coffee",
    "Fair Trade Organic Colombian Coffee",
    "Fair Trade Organic El Toro Loco Blend Coffee",
    "Fair Trade Organic Midnight in Macau Blend Coffee",
    "Fair Trade Organic Mystic Mojo Blend Coffee",
    "Fair Trade Organic Papua New Guinea Coffee",
    "Fair Trade Organic Peruvian Coffee",
    "Fair Trade Organic Three Eyed Buddha Blend Coffee",
    "Flavored Coffee",
    "French Caramel Fudge Flavored Coffee",
    "French Roast Blend Coffee",
    "French Toast Flavored Coffee",
    "French Vanilla Flavored Coffee",
    "Funkee Monkee Flavored Coffee",
    "German Chocolate Cake Flavored Coffee",
    "Gimme a S'mores Flavored Coffee",
    "Gingerbread Spice Flavored Coffee",
    "Goblin's Grogg Flavored Coffee",
    "Golden Peanut Brittle Flavored Coffee",
    "Gooey Caramel Fudge Brownie Flavored Coffee",
    "Google Search Results",
    "Graham Cracker Suite Flavored Coffee",
    "Grand Marnier Flavored Coffee",
    "Grandma's Chocolate Cinnamon Snicker Cookies Flavored Coffee",
    "Grandpa's Cinnamon Snicker Cookies Flavored Coffee",
    "Guatemalan Antiqua",
    "Harvest Spice Flavored Coffee",
    "Hawaiian Coconut Flavored Coffee",
    "Hawaiian Hazelnut Flavored Coffee",
    "Hawaiian Macadamia Nut Flavored Coffee",
    "Hazelnut Cream Flavored Coffee",
    "Hazelnut Flavored Coffee",
    "Hazelnut Strong & Nutty Flavored Coffee",
    "Highlander Grog Flavored Coffee",
    "Holiday Magic Flavored Coffee",
    "Holiday Spice Flavored Coffee",
]

countries = [
    "Afghanistan",
    "Albania",
    "Algeria",
    "Andorra",
    "Angola",
    "Antigua and Barbuda",
    "Argentina",
    "Armenia",
    "Australia",
    "Austria",
    "Azerbaijan",
    "Bahamas",
    "Bahrain",
    "Bangladesh",
    "Barbados",
    "Belarus",
    "Belgium",
    "Belize",
    "Benin",
    "Bhutan",
    "Bolivia",
    "Bosnia and Herzegovina",
    "Botswana",
    "Brazil",
    "Brunei",
    "Bulgaria",
    "Burkina Faso",
    "Burundi",
    "Cabo Verde",
    "Cambodia",
    "Cameroon",
    "Canada",
    "Central African Republic",
    "Chad",
    "Chile",
    "China",
    "Colombia",
    "Comoros",
    "Congo, Democratic Republic of the",
    "Congo, Republic of the",
    "Costa Rica",
    "Cote d'Ivoire",
    "Croatia",
    "Cuba",
    "Cyprus",
    "Czechia",
    "Denmark",
    "Djibouti",
    "Dominica",
    "Dominican Republic",
    "Ecuador",
    "Egypt",
    "El Salvador",
    "Equatorial Guinea",
    "Eritrea",
    "Estonia",
    "Eswatini",
    "Ethiopia",
    "Fiji",
    "Finland",
    "France",
    "Gabon",
    "Gambia",
    "Georgia",
    "Germany",
    "Ghana",
    "Greece",
    "Grenada",
    "Guatemala",
    "Guinea",
    "Guinea-Bissau",
    "Guyana",
    "Haiti",
    "Honduras",
    "Hungary",
    "Iceland",
    "India",
    "Indonesia",
    "Iran",
    "Iraq",
    "Ireland",
    "Israel",
    "Italy",
    "Jamaica",
    "Japan",
    "Jordan",
    "Kazakhstan",
    "Kenya",
    "Kiribati",
    "Kosovo",
    "Kuwait",
    "Kyrgyzstan",
    "Laos",
    "Latvia",
    "Lebanon",
    "Lesotho",
    "Liberia",
    "Libya",
    "Liechtenstein",
    "Lithuania",
    "Luxembourg",
    "Madagascar",
    "Malawi",
    "Malaysia",
    "Maldives",
    "Mali",
    "Malta",
    "Marshall Islands",
    "Mauritania",
    "Mauritius",
    "Mexico",
    "Micronesia",
    "Moldova",
    "Monaco",
    "Mongolia",
    "Montenegro",
    "Morocco",
    "Mozambique",
    "Myanmar",
    "Namibia",
    "Nauru",
    "Nepal",
    "Netherlands",
    "New Zealand",
    "Nicaragua",
    "Niger",
    "Nigeria",
    "North Korea",
    "North Macedonia",
    "Norway",
    "Oman",
    "Pakistan",
    "Palau",
    "Palestine",
    "Panama",
    "Papua New Guinea",
    "Paraguay",
    "Peru",
    "Philippines",
    "Poland",
    "Portugal",
    "Qatar",
    "Romania",
    "Russia",
    "Rwanda",
    "Saint Kitts and Nevis",
    "Saint Lucia",
    "Saint Vincent and the Grenadines",
    "Samoa",
    "San Marino",
    "Sao Tome and Principe",
    "Saudi Arabia",
    "Senegal",
    "Serbia",
    "Seychelles",
    "Sierra Leone",
    "Singapore",
    "Slovakia",
    "Slovenia",
    "Solomon Islands",
    "Somalia",
    "South Africa",
    "South Korea",
    "South Sudan",
    "Spain",
    "Sri Lanka",
    "Sudan",
    "Suriname",
    "Sweden",
    "Switzerland",
    "Syria",
    "Taiwan",
    "Tajikistan",
    "Tanzania",
    "Thailand",
    "Timor-Leste",
    "Togo",
    "Tonga",
    "Trinidad and Tobago",
    "Tunisia",
    "Turkey",
    "Turkmenistan",
    "Tuvalu",
    "Uganda",
    "Ukraine",
    "United Arab Emirates",
    "United Kingdom",
    "United States of America",
    "Uruguay",
    "Uzbekistan",
    "Vanuatu",
    "Vatican City",
    "Venezuela",
    "Vietnam",
    "Yemen",
    "Zambia",
    "Zimbabwe",
]


class DbmsSimulator(Component):
    def get_table(self, table_key):
        return self.tables.get(table_key)

    def set_table(self, table_key, table):
        self.tables[table_key] = table

    def __init__(self, schema_file_name, load_from_mem=False) -> None:
        super().__init__(LogType.DBMS_SIMULATOR)
        self.tables = {}
        self.transaction_manager = TransactionManager()
        self.scheduler = Scheduler()
        self.catalog_manager = CatalogManager(schema_file_name)
        if load_from_mem:
            with open("company_data.pkl", "rb") as inp:
                self.data_manager = pickle.load(inp)
        else:
            self.data_manager = DataManager(self.catalog_manager, self.tables, 4)
        self.log("DBMS Simulator component initialized.")


def test_B():
    pass


def test_C():
    pass


def test_A():
    pass


def test_Q():
    pass


def test_I(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 0 LATTE 5 USA")
    dbms.data_manager.insert("starbucks", 0, ("latte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 2 LATTE 5 ITALLIA")
    dbms.data_manager.insert("starbucks", 2, ("latte", 5, "ITALLIIIAAA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)
    pass


def test_U(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("UPDATING C_ID 1 TO INTENSITY 3")
    dbms.data_manager.update("starbucks", 1, 3)
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)
    pass


def test_R(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("\nSTART READ")
    x = dbms.data_manager.read("starbucks", 1)
    print("OUTPUT: ", x)
    print("END READ")
    pass


def test_T(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 0 LATTE 5 USA")
    dbms.data_manager.insert("starbucks", 0, ("latte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 2 LATTE 5 ITALLIA")
    dbms.data_manager.insert("starbucks", 2, ("latte", 5, "ITALLIIIAAA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("START TABLE READ")
    x = dbms.data_manager.table_read("starbucks")
    print("OUTPUT: ", x)
    print("END TABLE READ")
    pass


def test_M(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 0 LATTE 5 USA")
    dbms.data_manager.insert("starbucks", 0, ("latte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 3 MATTE 5 USA")
    dbms.data_manager.insert("starbucks", 3, ("matte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 2 MACHIATO 10 FRANCE")
    dbms.data_manager.insert("starbucks", 2, ("mochiato", 10, "France"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print

    print("START G OP")
    x = dbms.data_manager.op_m("starbucks", "USA")
    print("OUTPUT: ", x)
    print("END G OP")
    pass


def test_G(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    print("INSERTING 0 LATTE 5 USA")
    dbms.data_manager.insert("starbucks", 0, ("latte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 3 MATTE 5 USA")
    dbms.data_manager.insert("starbucks", 3, ("matte", 5, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 2 MACHIATO 10 FRANCE")
    dbms.data_manager.insert("starbucks", 2, ("mochiato", 10, "France"))
    print(dbms.data_manager.get_table("starbucks"))  # table print
    print(dbms.data_manager.col_cache)

    print("INSERTING 1 NITRO 12 USA")
    dbms.data_manager.insert("starbucks", 1, ("nitro", 12, "USA"))
    print(dbms.data_manager.get_table("starbucks"))  # table print

    print("START M OP")
    x = dbms.data_manager.op_m("starbucks", "USA")
    print("OUTPUT: ", x)
    print("END M OP")
    pass


def test_prim_idx(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    page_1 = (1, 20)
    page_2 = (10, 23)
    page_3 = (100, 41)
    page_4 = (1000, 620)
    page_5 = (10000, 21)
    page_6 = (100000, 57)
    primary_index = dbms.catalog_manager.get_auxiliary("table_key_1", "intensity")

    primary_index.recreate([page_1, page_2, page_3, page_4, page_5])
    primary_index.set(101, 50)
    primary_index.set(102, 50)
    primary_index.set(103, 50)
    print("pages", primary_index.page_numbers, primary_index.overflows)
    primary_index.set(105, None)
    primary_index.set(103, None)
    primary_index.set(1000, None)
    print("pages", primary_index.page_numbers, primary_index.overflows)
    print("page", primary_index.get(3))
    primary_index.recreate([page_2, page_3, page_5])
    print("pages!", primary_index.page_numbers)
    print("page", primary_index.get(101))
    pass


def test_cluster_idx(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)

    clstr_index = dbms.catalog_manager.get_auxiliary("table_key_1", "country_of_origin")

    clstr_index.set(("USA", 0))
    print(clstr_index.page_numbers.values())
    clstr_index.set(("France", 1))
    print(clstr_index.page_numbers.values())
    clstr_index(("USA", 2))
    print(clstr_index.page_numbers.values())

    pass


def test_agg(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    aggregate = dbms.catalog_manager.get_aggregate("table_key_1", "intensity")
    print("Increment 5")
    aggregate.increment(5)
    print("aggr result", aggregate.get(5))
    print("Decrement 5")
    aggregate.decrement(5)
    print("aggr result", aggregate.get(5))
    print("Decrement 5 again")
    aggregate.decrement(5)
    print("aggr result", aggregate.get(5))


def test_blm_fltr(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    blm_filter = dbms.catalog_manager.get_filter("t1", "coffee_id")
    blm_filter.add(1)
    print("Added one")
    print(" one is in bloom filter: ", 1 in blm_filter)
    print("zero in bloom filter: ", 0 in blm_filter)
    pass


def stress_test(schema_file_name):
    dbms = DbmsSimulator(schema_file_name)
    t_id = "starbucks"
    for i in range(512):
        dbms.data_manager.insert(
            t_id,
            i,
            (
                i,
                coffee_list[i % len(coffee_list) - 1],
                random.sample(range(0, 12 + 1), 1),
                countries[i % len(countries) - 1],
            ),
        )
    pass


def main():
    schema_file_name = sys.argv[1]
    transaction_processing_type = sys.argv[2]
    file_names = sys.argv[3:]
    dbms = DbmsSimulator(schema_file_name)
    file_txns = dbms.transaction_manager.read_files(file_names)
    # txns = [value for (key, value) in file_txns.items()]
    # serialized_txns = dbms.scheduler.execute(txns)

    # python final-project/DbmsSimulator.py final-project/schema.json final-project/files/sample1.txt > final-project/out.txt
    ph3 = True
    ph2 = False
    ph2_op_m = False
    ph2_op_g = False
    test_aux = False

    # python final-project/DbmsSimulator.py final-project/schema.json final-project/files/sample1.txt > final-project/out.txt
    if ph3:
        file_txns = dbms.transaction_manager.read_files(file_names)
        txns = [value for (key, value) in file_txns.items()]
        print("TXNS", txns)
        serialized_txns = dbms.scheduler.get_serialized_history(txns)

    if ph2_op_g:
        print("INSERTING 0 LATTE 5 USA")
        dbms.data_manager.insert("starbucks", 0, ("latte", 5, "USA"))
        print(dbms.data_manager.get_table("starbucks"))  # table print
        print(dbms.data_manager.col_cache)
    # txns = txn_mgr.read_files(file_names)
    # if txn_processing_type == 'rr':
    #     txn_mgr.round_robin(txns)
    # elif txn_processing_type == 'ran':
    #     txn_mgr.random_read(txns)


if __name__ == "__main__":
    main()
