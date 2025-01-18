import json
from collections import Counter
from sentence_transformers import SentenceTransformer, util
from difflib import get_close_matches

class ProductClassifier:
    def __init__(self, model_name='all-MiniLM-L6-v2', threshold=0.6):
        self.model = SentenceTransformer(model_name)
        self.threshold = threshold
        self.ignore_words = {'pams', 'woolworths', 'value', 'kg', 'g', 'ml', 'l', 'pack', 'pk', 'ea'}
        
        # Store embeddings for keywords
        self.keyword_embeddings = {}
        
        self.product_type_keywords = {
            # Dairy & Eggs
            'Dairy & Eggs': [
                'milk', 'whole milk', 'skim milk', 'low-fat milk', 'butter', 
                'unsalted butter', 'salted butter', 'cheese', 'cheddar', 'mozzarella', 
                'parmesan', 'gouda', 'feta', 'brie', 'camembert', 'blue cheese', 
                'cream cheese', 'goat cheese', 'ricotta', 'yogurt', 'Greek yogurt', 
                'flavored yogurt', 'plain yogurt', 'cream', 'heavy cream', 
                'whipping cream', 'double cream', 'sour cream', 'custard', 
                'margarine', 'eggs', 'chicken eggs', 'duck eggs', 'quail eggs', 
                'buttermilk', 'kefir', 'curd', 'paneer', 'ghee', 'spread', 
                'eggwhite', 'egg yolk', 'powdered milk', 'condensed milk', 
                'evaporated milk', 'milkshake', 'ice cream', 'frozen yogurt', 
                'whey', 'lactose-free milk', 'almond milk', 'soy milk', 
                'oat milk', 'coconut milk', 'cashew milk', 'milk powder', 
                'clarified butter', 'probiotic drinks', 'quark', 'clotted cream'
            ],

            # Bread & Bakery
            'Bread & Bakery': [
                'bread', 'white bread', 'whole wheat bread', 'multigrain bread', 
                'rye bread', 'sourdough', 'pita bread', 'ciabatta', 'focaccia', 
                'roll', 'dinner roll', 'bun', 'burger bun', 'hot dog bun', 
                'bagel', 'plain bagel', 'sesame bagel', 'everything bagel', 
                'muffin', 'blueberry muffin', 'chocolate chip muffin', 
                'croissant', 'almond croissant', 'pastry', 'danish', 'eclair', 
                'strudel', 'cake', 'chocolate cake', 'vanilla cake', 'sponge cake', 
                'fruit cake', 'loaf', 'banana bread', 'pumpkin bread', 
                'baguette', 'crumpet', 'waffle', 'Belgian waffle', 'pancake', 
                'donut', 'glazed donut', 'chocolate donut', 'doughnut', 'pie', 
                'apple pie', 'cherry pie', 'pumpkin pie', 'tart', 'fruit tart', 
                'custard tart', 'scone', 'plain scone', 'raisin scone', 
                'brioche', 'flatbread', 'naan', 'paratha', 'chapati', 
                'lavash', 'rolls', 'buns', 'wrap', 'tortilla', 'flour tortilla', 
                'corn tortilla', 'cinnamon roll', 'pretzel', 'breadsticks', 
                'English muffin', 'hot cross bun', 'shortbread', 'biscuit', 
                'cracker', 'grissini', 'pavlova', 'macaron', 'cookie', 
                'gingerbread', 'puff pastry', 'challah', 'matzo', 'baps', 
                'wholemeal', 'batard','turnovers'
            ],

            # Beverages
           'Beverages': [
                'juice', 'orange juice', 'apple juice', 'grape juice', 
                'cranberry juice', 'pineapple juice', 'tomato juice', 
                'pomegranate juice', 'carrot juice', 'beet juice', 'water', 
                'sparkling water', 'mineral water', 'flavored water', 'coffee', 
                'black coffee', 'espresso', 'latte', 'cappuccino', 'americano', 
                'macchiato', 'mocha', 'iced coffee', 'cold brew', 'tea', 
                'black tea', 'green tea', 'herbal tea', 'chai', 'matcha', 
                'iced tea', 'drink', 'energy drink', 'sports drink', 'soda', 
                'cola', 'lemon-lime soda', 'root beer', 'ginger ale', 'tonic water', 
                'club soda', 'pop', 'beverage', 'smoothie', 'fruit smoothie', 
                'protein shake', 'cocktail', 'martini', 'margarita', 'mojito', 
                'pina colada', 'daiquiri', 'bloody mary', 'wine', 'red wine', 
                'white wine', 'rosé wine', 'sparkling wine', 'champagne', 
                'beer', 'ale', 'lager', 'stout', 'porter', 'pilsner', 
                'cider', 'hard cider', 'spirits', 'liquor', 'vodka', 
                'flavored vodka', 'gin', 'rum', 'dark rum', 'white rum', 
                'whiskey', 'bourbon', 'scotch', 'rye whiskey', 'cordial', 
                'syrup', 'simple syrup', 'grenadine', 'concentrate', 'shake', 
                'milkshake', 'chocolate milkshake', 'strawberry milkshake', 
                'bubble tea', 'kombucha', 'matcha latte', 'hot chocolate', 
                'chai latte', 'iced matcha', 'tonic', 'lemonade', 'limeade'
            ],

            # Pantry Staples
            'Pantry Items': [
                'sugar', 'white sugar', 'brown sugar', 'powdered sugar', 
                'salt', 'sea salt', 'kosher salt', 'pink Himalayan salt', 
                'flour', 'all-purpose flour', 'whole wheat flour', 'bread flour', 
                'oil', 'olive oil', 'vegetable oil', 'canola oil', 'coconut oil', 
                'sunflower oil', 'avocado oil', 'sesame oil', 'vinegar', 
                'white vinegar', 'apple cider vinegar', 'balsamic vinegar', 
                'rice vinegar', 'red wine vinegar', 'sauce', 'soy sauce', 
                'hot sauce', 'BBQ sauce', 'tomato sauce', 'paste', 'tomato paste', 
                'chili paste', 'garlic paste', 'soup', 'stock', 'chicken stock', 
                'beef stock', 'vegetable stock', 'broth', 'seasoning', 
                'spice', 'herb', 'basil', 'oregano', 'thyme', 'extract', 
                'vanilla extract', 'almond extract', 'essence', 'powder', 
                'garlic powder', 'onion powder', 'cocoa powder', 'mix', 
                'pancake mix', 'cake mix', 'marinade', 'glaze', 'dressing', 
                'ranch dressing', 'Italian dressing', 'condiment', 'mayo', 
                'mayonnaise', 'mustard', 'Dijon mustard', 'whole grain mustard', 
                'ketchup', 'relish', 'chutney', 'jam', 'strawberry jam', 
                'apricot jam', 'jelly', 'preserves', 'honey', 'maple syrup', 
                'syrup', 'peanut butter', 'almond butter', 'Nutella', 'marmite', 
                'vegemite', 'tahini', 'molasses', 'cornstarch', 'yeast', 
                'baking soda', 'baking powder', 'coriander', 'cumin', 'parsley',
                'pesto', 'tzatziki'
            ],
            # Grains & Pasta
            'Grains & Pasta': [
                'cereal', 'cornflakes', 'bran flakes', 'pasta', 'spaghetti', 
                'penne', 'linguine', 'fettuccine', 'macaroni', 'lasagna', 
                'ravioli', 'tortellini', 'angel hair pasta', 'ziti', 
                'rice', 'white rice', 'brown rice', 'basmati rice', 'jasmine rice', 
                'wild rice', 'noodle', 'egg noodle', 'ramen', 'udon', 'soba', 
                'grain', 'quinoa', 'couscous', 'oats', 'steel-cut oats', 
                'rolled oats', 'instant oatmeal', 'porridge', 'muesli', 
                'granola', 'wheat', 'bulgur wheat', 'barley', 'cornmeal', 
                'polenta', 'semolina', 'flour', 'buckwheat flour', 'meal', 
                'bran', 'millet', 'amaranth', 'teff', 'sorghum'
            ],

            # Snacks & Confectionery
            'Snacks & Confectionery': [
                'chips', 'potato chips', 'tortilla chips', 'crisps', 
                'crackers', 'whole grain crackers', 'cheese crackers', 
                'cookies', 'chocolate chip cookies', 'oatmeal cookies', 
                'biscuit', 'digestive biscuits', 'shortbread', 'wafer', 
                'popcorn', 'buttered popcorn', 'caramel popcorn', 'nuts', 
                'almonds', 'cashews', 'walnuts', 'peanuts', 'pistachios', 
                'chocolate', 'milk chocolate', 'dark chocolate', 'white chocolate', 
                'candy', 'hard candy', 'chewy candy', 'lollies', 'sweets', 
                'gum', 'mints', 'bar', 'granola bar', 'energy bar', 
                'snack', 'pretzel', 'soft pretzel', 'nachos', 'dip', 
                'guacamole', 'salsa', 'hummus', 'trail mix', 'granola', 
                'fruit snacks', 'marshmallows', 'toffee', 'fudge', 'licorice',
                'biersticks'
            ],

            # Fruits & Vegetables
            'Fruits & Vegetables': [
                'apple', 'banana', 'orange', 
                'lemon', 'lime', 'grape', 'berry', 'berries', 
                'strawberry', 'blueberry', 'raspberry', 'blackberry', 
                'cranberry', 'gooseberry', 'boysenberry', 'huckleberry', 
                'melon', 'watermelon', 'cantaloupe', 'honeydew', 
                'pineapple', 'mango', 'peach', 'plum', 'pear', 
                'apricot', 'nectarine', 'fig', 'date', 'raisin', 
                'currant', 'sultana', 'pomegranate', 'kiwi', 
                'papaya', 'guava', 'passionfruit', 'dragonfruit', 
                'lychee', 'longan', 'persimmon', 'starfruit', 
                'jackfruit', 'durian', 'coconut', 'avocado', 
                'tomato', 'potato', 'sweet potato', 'carrot', 
                'onion', 'garlic', 'shallot', 'leek', 'lettuce', 
                'cabbage', 'broccoli', 'cauliflower', 'brussels sprout', 
                'pepper', 'bell pepper', 'chili pepper', 'cucumber', 
                'zucchini', 'courgette', 'celery', 'asparagus', 
                'mushroom', 'corn', 'sweetcorn', 'pea', 'bean', 
                'green bean', 'snow pea', 'sugar snap pea', 
                'edamame', 'chickpea', 'lentil', 'sprout', 
                'spinach', 'kale', 'collard greens', 'mustard greens', 
                'turnip', 'beet', 'radish', 'rutabaga', 
                'parsnip', 'swede', 'yam', 'eggplant', 'artichoke', 
                'fennel', 'okra', 'bamboo shoot', 'watercress', 
                'seaweed', 'arugula', 'chard', 'bok choy', 
                'daikon', 'jicama', 'horseradish', 'pumpkin', 
                'squash', 'acorn squash', 'butternut squash', 
                'spaghetti squash', 'gourd', 'taro', 'cassava',
                'mandarins', 'slaw', 'rocket','pitahaya','dragonfruit',
                'paw paw'
            ],

            # Meat & Seafood
            'Meat & Seafood': [
                'meat', 'red meat', 'beef', 'ground beef', 'steak', 'ribeye steak', 
                'sirloin steak', 'pork', 'pork chops', 'pork loin', 'lamb', 
                'lamb chops', 'leg of lamb', 'chicken', 'chicken breast', 
                'chicken thighs', 'chicken wings', 'whole chicken', 'turkey', 
                'ground turkey', 'turkey breast', 'duck', 'duck breast', 
                'bacon', 'pork bacon', 'turkey bacon', 'ham', 'cooked ham', 
                'honey-glazed ham', 'sausage', 'beef sausage', 'pork sausage', 
                'turkey sausage', 'salami', 'pepperoni', 'mince', 'ground meat', 
                'veal', 'game meat', 'venison', 'rabbit', 'steak', 'chop', 
                'lamb chop', 'pork chop', 'roast', 'beef roast', 'pork roast', 
                'fillet', 'fish', 'white fish', 'salmon', 'smoked salmon', 
                'tuna', 'canned tuna', 'fresh tuna', 'cod', 'haddock', 
                'tilapia', 'snapper', 'mackerel', 'prawns', 'shrimp', 
                'jumbo shrimp', 'shellfish', 'mussels', 'oysters', 'clams', 
                'scallops', 'crab', 'king crab', 'crab legs', 'lobster', 
                'lobster tail', 'seafood', 'calamari', 'octopus', 'anchovies', 
                'sardines', 'fish fingers', 'fish fillet', 'crayfish', 'roe',
                'frankfurters', 'chorizo', 'saveloys', 'franks', 'rissoles', 
                'tenderloins', 'pastrami','sizzlers'
            ],

            # Frozen Foods
            'Frozen Foods': [
                'ice cream', 'vanilla ice cream', 'chocolate ice cream', 
                'strawberry ice cream', 'gelato', 'sorbet', 'lemon sorbet', 
                'mango sorbet', 'frozen yogurt', 'froyo', 'frozen pizza', 
                'pepperoni pizza', 'vegetarian pizza', 'frozen meal', 
                'frozen dinner', 'TV dinner', 'microwave meal', 
                'frozen dessert', 'popsicle', 'ice pop', 'frozen fruit', 
                'frozen berries', 'frozen peas', 'frozen corn', 'frozen vegetables', 
                'ice', 'crushed ice', 'ice cubes', 'frozen waffles', 
                'frozen pancakes', 'frozen pastries', 'frozen pie', 'pot pies', 
                'frozen dumplings', 'frozen spring rolls', 'frozen seafood', 
                'frozen shrimp', 'frozen fish fillets', 'frozen chicken nuggets', 
                'frozen fries', 'frozen chips', 'frozen bread dough'
            ],
            # Canned & Packaged Foods
            'Canned & Packaged Foods': [
                'soup', 'chicken soup', 'tomato soup', 'vegetable soup', 
                'beans', 'baked beans', 'kidney beans', 'black beans', 
                'chickpeas', 'lentils', 'tomatoes', 'diced tomatoes', 
                'crushed tomatoes', 'tomato paste', 'corn', 'sweet corn', 
                'cream-style corn', 'peas', 'green peas', 'fruit', 'canned fruit', 
                'peaches', 'pineapple', 'fruit cocktail', 'tuna', 'canned tuna', 
                'salmon', 'canned salmon', 'sardines', 'canned sardines', 
                'anchovies', 'meal', 'ready-to-eat meal', 'instant noodles', 
                'macaroni and cheese', 'dinner', 'pasta', 'instant pasta', 
                'sauce', 'tomato sauce', 'alfredo sauce', 'vegetables', 
                'mixed vegetables', 'spinach', 'artichokes', 'olives', 'mix', 
                'pancake mix', 'muffin mix', 'cake mix', 'cornbread mix', 
                'stuffing mix', 'noodles', 'rice', 'canned gravy', 'broth', 
                'chicken broth', 'beef broth'
            ],

            # Baby & Infant
            'Baby & Infant': [
                'formula', 'infant formula', 'toddler formula', 'food', 
                'baby food', 'stage 1 baby food', 'stage 2 baby food', 
                'puree', 'fruit puree', 'vegetable puree', 'snack', 
                'baby snack', 'teething biscuits', 'puffs', 'cereal', 
                'baby cereal', 'rice cereal', 'oatmeal cereal', 'juice', 
                'baby juice', 'apple juice', 'pear juice', 'milk', 
                'toddler milk', 'baby yogurt', 'baby pudding'
            ],

            # Pet Food
            'Pet Food': [
                'food', 'dog food', 'cat food', 'puppy food', 'kitten food', 
                'wet food', 'canned food', 'dry food', 'treats', 
                'dog treats', 'cat treats', 'kibble', 'dry kibble', 
                'biscuits', 'dog biscuits', 'cat biscuits', 'feed', 
                'bird feed', 'fish food', 'rabbit feed', 'hamster feed', 
                'pellets', 'grain-free food', 'high-protein food', 
                'senior pet food', 'special diet food'
            ],

            # Health & Wellness
            'Health & Wellness': [
                'supplement', 'dietary supplement', 'multivitamin', 'vitamin', 
                'vitamin C', 'vitamin D', 'vitamin B12', 'protein', 'protein powder', 
                'whey protein', 'plant-based protein', 'collagen', 'amino acids', 
                'powder', 'greens powder', 'superfood powder', 'bar', 'protein bar', 
                'energy bar', 'meal replacement bar', 'shake', 'protein shake', 
                'meal replacement shake', 'smoothie mix', 'tablet', 'chewable tablet', 
                'capsule', 'softgel', 'gummy', 'omega-3 gummies', 'fiber gummies', 
                'oil', 'fish oil', 'flaxseed oil', 'CBD oil', 'essential oil', 
                'immune booster', 'detox supplement', 'herbal supplement'
            ],

            # Cleaning & Household
           'Cleaning & Household': [
                'cleaner', 'all-purpose cleaner', 'glass cleaner', 'bathroom cleaner', 
                'floor cleaner', 'detergent', 'laundry detergent', 'dish detergent', 
                'soap', 'dish soap', 'hand soap', 'bar soap', 'powder', 'laundry powder', 
                'cleaning powder', 'liquid', 'cleaning liquid', 'detergent liquid', 
                'spray', 'disinfectant spray', 'air freshener spray', 'wipes', 
                'disinfectant wipes', 'baby wipes', 'surface wipes', 'bleach', 
                'toilet bleach', 'household bleach', 'freshener', 'air freshener', 
                'odor eliminator', 'paper', 'paper towel', 'toilet paper', 
                'tissue paper', 'towel', 'kitchen towel', 'bath towel', 'tissue', 
                'facial tissue', 'wrap', 'cling wrap', 'plastic wrap', 'bag', 
                'garbage bag', 'reusable bag', 'foil', 'aluminum foil', 'filter', 
                'water filter', 'air filter', 'vacuum bag', 'dryer sheet'
            ],

            # Personal Care
           'Personal Care': [
                'shampoo', 'anti-dandruff shampoo', 'volumizing shampoo', 
                'conditioner', 'deep conditioner', 'leave-in conditioner', 
                'soap', 'bar soap', 'liquid soap', 'wash', 'body wash', 
                'face wash', 'lotion', 'body lotion', 'hand lotion', 'cream', 
                'moisturizing cream', 'anti-aging cream', 'deodorant', 'stick deodorant', 
                'spray deodorant', 'toothpaste', 'whitening toothpaste', 
                'sensitive toothpaste', 'mouthwash', 'antibacterial mouthwash', 
                'floss', 'dental floss', 'floss picks', 'brush', 'toothbrush', 
                'hairbrush', 'razor', 'disposable razor', 'electric razor', 
                'tissue', 'facial tissue', 'wipes', 'makeup wipes', 'baby wipes', 
                'sanitizer', 'hand sanitizer', 'spray sanitizer', 'sunscreen', 
                'SPF moisturizer', 'sunblock', 'lip balm', 'nail clippers', 'cotton swabs'
            ],

            # Miscellaneous
            'Miscellaneous': [
                'set', 'gift set', 'starter set', 'pack', 'multi-pack', 
                'value pack', 'kit', 'starter kit', 'travel kit', 'bundle', 
                'product bundle', 'collection', 'gift collection', 'variety', 
                'variety pack', 'selection', 'curated selection', 'assortment', 
                'mixed assortment', 'mix', 'trail mix', 'combo', 'combo pack', 
                'package', 'care package', 'gift package', 'gift', 'gift card', 
                'gift basket', 'subscription box'
            ]

        }

        # Create reverse mapping with plural forms and embeddings for keywords
        self.keyword_to_category = {}
        for category, keywords in self.product_type_keywords.items():
            for keyword in keywords:
                # Add original form
                self.keyword_to_category[keyword] = category
                # Add plural forms
                self.keyword_to_category[keyword + 's'] = category
                # Handle special cases
                if keyword.endswith('y'):
                    self.keyword_to_category[keyword[:-1] + 'ies'] = category
                elif keyword.endswith('f'):
                    self.keyword_to_category[keyword[:-1] + 'ves'] = category
                
                # Create embedding for each keyword
                self.keyword_embeddings[keyword] = self.model.encode(keyword, convert_to_tensor=True)

    def get_last_word(self, product_name):
        """Extract the last word from the product name, ignoring specified terms."""
        # Remove any parentheses content and handle other punctuation
        name = product_name.split('(')[0].strip()
        name = name.replace('-', ' ')
        
        # Split into words
        words = name.lower().split()
        
        # Remove ignored words and size specifications
        words = [w for w in words if w not in self.ignore_words and not any(c.isdigit() for c in w)]
        
        # If 'half' or 'base' is found, ignore it and return the next meaningful word
        if words[-1] in ['half', 'base']:
            return words[-2] if len(words) > 1 else words[-1]
        
        return words[-1] if words else ''

    def find_category(self, word):
        """Try to find category for a word using multiple methods."""
        # Method 1: Direct lookup including plural forms
        category = self.keyword_to_category.get(word)
        if category:
            return category, word, 1.0, 'exact'

        # Method 2: Fuzzy string matching
        all_keywords = list(self.keyword_embeddings.keys())
        close_matches = get_close_matches(word, all_keywords, n=1, cutoff=0.8)
        if close_matches:
            matched_word = close_matches[0]
            return self.keyword_to_category[matched_word], matched_word, 0.9, 'fuzzy'

        # Method 3: Semantic similarity using SBERT
        if word:
            word_embedding = self.model.encode(word, convert_to_tensor=True)
            best_score = 0
            best_match = None
            
            for keyword, keyword_embedding in self.keyword_embeddings.items():
                similarity = util.cos_sim(word_embedding, keyword_embedding).item()
                if similarity > best_score:
                    best_score = similarity
                    best_match = keyword

            if best_score > self.threshold:
                return (self.keyword_to_category[best_match], best_match, 
                        best_score, 'semantic')

        return None, word, 0.0, 'none'

    def get_all_words(self, product_name):
        """Extract all relevant words from the product name, ignoring specified terms."""
        name = product_name.split('(')[0].strip()  # Remove anything in parentheses
        name = name.replace('-', ' ')  # Handle hyphenated words

        # Split into words
        words = name.lower().split()

        # Remove ignored words and size specifications
        words = [w for w in words if w not in self.ignore_words and not any(c.isdigit() for c in w)]
        
        return words

    def classify_products(self, input_file, output_file):
        try:
            with open(input_file, 'r', encoding='utf-8') as file:
                products = json.load(file)

            matched_products = []
            unmatched_products = []
            
            for product in products:
                name = product.get('name', '')
                words = self.get_all_words(name)
                
                # Attempt to match the last word first
                last_word = words[-1] if words else ''
                category, matched_word, confidence, match_type = self.find_category(last_word)
                
                # If no match with the last word, attempt to match any other word
                if not category:
                    for word in words[:-1]:  # Exclude the last word already tried
                        category, matched_word, confidence, match_type = self.find_category(word)
                        if category:
                            break  # Stop if we find a match

                # Assign classification results
                if category:
                    product['classifiedType'] = category
                    product['classificationConfidence'] = round(confidence, 3)
                    product['matchedWord'] = matched_word
                    product['originalWord'] = last_word
                    product['matchType'] = match_type
                    matched_products.append(product)
                else:
                    product['classifiedType'] = 'Unknown'
                    product['classificationConfidence'] = 0.0
                    product['matchedWord'] = last_word
                    product['matchType'] = 'none'
                    unmatched_products.append(product)

            # Save results
            with open(output_file, 'w', encoding='utf-8') as file:
                json.dump(products, file, indent=4, ensure_ascii=False)

            # Print statistics
            total = len(products)
            print(f"\nClassification Results:")
            print(f"Total products: {total}")
            print(f"Matched: {len(matched_products)} ({len(matched_products)/total*100:.1f}%)")
            print("Match types:")
            match_types = Counter(p['matchType'] for p in matched_products)
            for match_type, count in match_types.items():
                print(f"  - {match_type}: {count} ({count/total*100:.1f}%)")
            print(f"Unmatched: {len(unmatched_products)} ({len(unmatched_products)/total*100:.1f}%)")

            # Save unmatched analysis
            self.save_unmatched_analysis(unmatched_products)

        except Exception as e:
            print(f"Error in classify_products: {str(e)}")
            raise

    def save_unmatched_analysis(self, unmatched_products):
        """Save unmatched keywords, their counts, and associated product names to a file."""
        try:
            # Extract unmatched words along with the corresponding full product names
            unmatched_data = []
            for product in unmatched_products:
                matched_word = product['matchedWord']
                full_name = product['name']
                unmatched_data.append({
                    'matchedWord': matched_word,
                    'fullProductName': full_name
                })

            # Count occurrences of unmatched words
            unmatched_counts = Counter([item['matchedWord'] for item in unmatched_data])

            # Save analysis to a file, including product names
            unmatched_output_file = 'unmatched_keywords_with_product_names.json'
            with open(unmatched_output_file, 'w', encoding='utf-8') as file:
                json.dump({
                    'counts': unmatched_counts,
                    'products': unmatched_data
                }, file, indent=4, ensure_ascii=False)

            print(f"Unmatched analysis saved to {unmatched_output_file}")
        except Exception as e:
            print(f"Error in save_unmatched_analysis: {str(e)}")
            raise



# Example usage
if __name__ == "__main__":
    classifier = ProductClassifier()
    input_file = 'paknsave_products_2025-01-14_11-10-47.json'
    output_file = 'classified_products_enhanced.json'
    
    classifier.classify_products(input_file, output_file)