# scripts/support_tickets.py
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()
Faker.seed(42)  # Reproducible results

def generate_support_tickets(users_df: pd.DataFrame, orders_df: pd.DataFrame) -> pd.DataFrame:
    """Generate customer service interactions and support tickets"""
    
    # Not all users contact support - sample 25% of users
    support_users = users_df.sample(frac=0.25, random_state=42)
    
    # Define realistic support categories and their frequency
    issue_categories = ['shipping_delay', 'product_defect', 'return_request', 'billing_question', 'account_issue', 'general_inquiry']
    issue_weights = [0.25, 0.20, 0.18, 0.15, 0.12, 0.10]
    
    # Priority levels and their distribution
    priority_options = ['low', 'medium', 'high']
    priority_weights = [0.50, 0.35, 0.15]
    
    # Status distribution (realistic for resolved tickets)
    status_options = ['resolved', 'closed', 'in_progress', 'open']
    status_weights = [0.70, 0.20, 0.08, 0.02]
    
    support_tickets = []
    
    for _, user in support_users.iterrows():
        user_id = user['user_id']
        registration_date = pd.to_datetime(user['registration_date'])
        
        # Get user's orders for context
        user_orders = orders_df[orders_df['user_id'] == user_id]
        
        # Generate 1-4 support tickets per user (weighted toward fewer tickets)
        num_tickets = np.random.choice([1, 2, 3, 4], p=[0.5, 0.3, 0.15, 0.05])
        
        for ticket_num in range(num_tickets):
            # Ticket timing - can be anytime after registration
            ticket_date = fake.date_between(
                start_date=registration_date.date(),
                end_date=datetime.now().date()
            )
            
            # Select issue category with realistic weights
            issue_category = np.random.choice(issue_categories, p=issue_weights)
            
            # Select priority (shipping delays and defects tend to be higher priority)
            if issue_category == 'shipping_delay':
                priority = np.random.choice(['medium', 'high'], p=[0.6, 0.4])
            elif issue_category == 'product_defect':
                priority = np.random.choice(['medium', 'high'], p=[0.5, 0.5])
            else:
                priority = np.random.choice(priority_options, p=priority_weights)
            
            # Select status with realistic distribution
            status = np.random.choice(status_options, p=status_weights)
            
            # Resolution time based on priority and issue type
            resolution_hours = calculate_resolution_time(issue_category, priority, status)
            
            # Satisfaction score (1-5, higher for resolved issues)
            if status in ['resolved', 'closed']:
                # Resolved tickets tend to have higher satisfaction
                satisfaction_score = np.random.choice([3, 4, 5], p=[0.2, 0.5, 0.3])
            elif status == 'in_progress':
                # In progress tickets have neutral satisfaction
                satisfaction_score = np.random.choice([2, 3, 4], p=[0.3, 0.5, 0.2])
            else:  # open tickets
                # Open tickets tend to have lower satisfaction
                satisfaction_score = np.random.choice([1, 2, 3], p=[0.4, 0.4, 0.2])
            
            # Generate ticket description based on category
            ticket_description = generate_ticket_description(issue_category, user_orders)
            
            # Agent assignment (simulate support team)
            assigned_agent = fake.random_element([
                'Sarah Johnson', 'Mike Chen', 'Lisa Rodriguez', 
                'David Kim', 'Emma Thompson', 'Alex Martinez'
            ])
            
            # Channel (how customer contacted support)
            contact_channel = np.random.choice(
                ['email', 'live_chat', 'phone', 'contact_form'], 
                p=[0.45, 0.30, 0.15, 0.10]
            )
            
            support_tickets.append({
                'ticket_id': f'TICK_{fake.random_int(100000, 999999)}',
                'user_id': user_id,
                'created_date': ticket_date,
                'issue_category': issue_category,
                'priority': priority,
                'status': status,
                'assigned_agent': assigned_agent,
                'contact_channel': contact_channel,
                'ticket_description': ticket_description,
                'resolution_time_hours': resolution_hours,
                'satisfaction_score': satisfaction_score,
                'is_escalated': fake.boolean(chance_of_getting_true=15),  # 15% escalation rate
                'follow_up_required': fake.boolean(chance_of_getting_true=25),  # 25% need follow-up
                'resolved_date': calculate_resolved_date(ticket_date, resolution_hours, status),
                'first_response_time_minutes': calculate_first_response_time(priority, contact_channel)
            })
    
    return pd.DataFrame(support_tickets)

def calculate_resolution_time(issue_category: str, priority: str, status: str) -> float:
    """Calculate realistic resolution time based on issue type and priority"""
    
    # Base resolution times by category (in hours)
    base_times = {
        'shipping_delay': 12,
        'product_defect': 24,
        'return_request': 8,
        'billing_question': 6,
        'account_issue': 4,
        'general_inquiry': 2
    }
    
    # Priority multipliers
    priority_multipliers = {
        'high': 0.5,    # High priority resolved faster
        'medium': 1.0,
        'low': 1.5      # Low priority takes longer
    }
    
    base_time = base_times[issue_category]
    multiplier = priority_multipliers[priority]
    
    # Add some randomness
    resolution_time = base_time * multiplier * fake.random.uniform(0.7, 1.3)
    
    # Unresolved tickets don't have resolution time
    if status in ['open', 'in_progress']:
        return None
    
    return round(resolution_time, 1)

def calculate_resolved_date(created_date, resolution_hours: float, status: str):
    """Calculate when ticket was resolved"""
    
    if status in ['open', 'in_progress'] or resolution_hours is None:
        return None
    
    resolved_date = pd.to_datetime(created_date) + timedelta(hours=resolution_hours)
    return resolved_date.date()

def calculate_first_response_time(priority: str, channel: str) -> int:
    """Calculate first response time in minutes"""
    
    # Base response times by channel (in minutes)
    channel_times = {
        'live_chat': 5,
        'phone': 2,      # Immediate for phone
        'email': 120,    # 2 hours
        'contact_form': 180  # 3 hours
    }
    
    # Priority adjustments
    priority_adjustments = {
        'high': 0.3,     # Much faster for high priority
        'medium': 1.0,
        'low': 2.0       # Slower for low priority
    }
    
    base_time = channel_times[channel]
    adjustment = priority_adjustments[priority]
    
    response_time = base_time * adjustment * fake.random.uniform(0.5, 1.5)
    return max(1, round(response_time))  # Minimum 1 minute

def generate_ticket_description(issue_category: str, user_orders: pd.DataFrame) -> str:
    """Generate realistic ticket descriptions based on issue category"""
    
    descriptions = {
        'shipping_delay': [
            "My order was supposed to arrive 3 days ago but tracking shows it's still in transit",
            "Package has been stuck at shipping facility for over a week",
            "Expected delivery date has passed, need update on my order status",
            "Tracking information hasn't updated in 5 days, is my package lost?"
        ],
        'product_defect': [
            "Received item with manufacturing defect, requesting replacement",
            "Product arrived damaged in packaging, multiple scratches visible",
            "Item doesn't match description on website, wrong specifications",
            "Quality issue with product, stopped working after 2 days of use"
        ],
        'return_request': [
            "Would like to return item, doesn't fit as expected",
            "Product not suitable for my needs, requesting return authorization",
            "Received wrong size, need to exchange for correct size",
            "Item doesn't meet expectations, would like full refund"
        ],
        'billing_question': [
            "Charged twice for the same order, need refund for duplicate charge",
            "Discount code didn't apply at checkout, can you adjust my bill?",
            "Subscription billing question, unexpected charge on my account",
            "Payment method was charged but order shows as pending"
        ],
        'account_issue': [
            "Cannot log into my account, password reset not working",
            "Account was suspended, need help reactivating",
            "Email preferences not saving, still receiving unwanted newsletters",
            "Unable to update shipping address in account settings"
        ],
        'general_inquiry': [
            "Question about product compatibility with other items",
            "When will this item be back in stock?",
            "Do you ship to my location? Need shipping information",
            "Looking for product recommendations for my specific needs"
        ]
    }
    
    category_descriptions = descriptions.get(issue_category, ["General customer inquiry"])
    return fake.random_element(category_descriptions)

if __name__ == "__main__":
    # Test the function with sample data
    sample_users = pd.DataFrame({
        'user_id': range(1, 101),
        'email': [f'user{i}@test.com' for i in range(1, 101)],
        'registration_date': [fake.date_between(start_date='-2y', end_date='today') for _ in range(100)]
    })
    
    sample_orders = pd.DataFrame({
        'order_id': range(1, 201),
        'user_id': [fake.random_int(1, 100) for _ in range(200)],
        'order_date': [fake.date_between(start_date='-1y', end_date='today') for _ in range(200)]
    })
    
    tickets = generate_support_tickets(sample_users, sample_orders)
    print(f"Generated {len(tickets)} support tickets")
    print(tickets.head())