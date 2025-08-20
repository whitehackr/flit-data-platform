import pytest
import pandas as pd
import sys
import os
from datetime import datetime

# Add scripts directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'scripts'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))  # Project root

from support_tickets import (
    generate_support_tickets, 
    calculate_resolution_time, 
    calculate_first_response_time,
    generate_ticket_description
)
from tests.fixtures.sample_data import sample_users_df, sample_orders_df, create_minimal_test_data

class TestSupportTicketGeneration:
    """Test support ticket generation functionality"""
    
    def test_generate_with_sample_data(self, sample_users_df, sample_orders_df):
        """Test ticket generation with known sample data"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        # Basic structure validation
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        
        # Should generate some tickets (not all users contact support)
        assert len(result_df) >= 0, "Should generate non-negative number of tickets"
        
        if len(result_df) > 0:  # If any tickets generated
            # Check required columns exist
            required_columns = [
                'ticket_id', 'user_id', 'created_date', 'issue_category',
                'priority', 'status', 'assigned_agent', 'contact_channel',
                'ticket_description', 'satisfaction_score'
            ]
            for col in required_columns:
                assert col in result_df.columns, f"Missing required column: {col}"
    
    def test_user_sampling(self, sample_users_df, sample_orders_df):
        """Test that only subset of users contact support"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            # Should be subset of input users (25% sample rate)
            unique_support_users = set(result_df['user_id'])
            all_users = set(sample_users_df['user_id'])
            
            assert unique_support_users.issubset(all_users), "Support users should be subset of all users"
            
            # Should be reasonable number (not all users)
            support_rate = len(unique_support_users) / len(all_users)
            assert 0 <= support_rate <= 0.5, f"Support rate {support_rate:.1%} seems unrealistic"

class TestIssueCategories:
    """Test issue category assignment"""
    
    def test_issue_categories_valid(self, sample_users_df, sample_orders_df):
        """Test that issue categories are from valid set"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            expected_categories = [
                'shipping_delay', 'product_defect', 'return_request',
                'billing_question', 'account_issue', 'general_inquiry'
            ]
            actual_categories = set(result_df['issue_category'])
            
            assert actual_categories.issubset(set(expected_categories)), \
                f"Invalid issue categories: {actual_categories - set(expected_categories)}"
    
    def test_issue_distribution_realistic(self, sample_users_df, sample_orders_df):
        """Test that issue distribution follows expected weights"""
        # Generate larger sample for better statistics
        large_users = pd.concat([sample_users_df] * 10, ignore_index=True)  # 100 users
        large_users['user_id'] = range(1, len(large_users) + 1)  # Unique IDs
        
        result_df = generate_support_tickets(large_users, sample_orders_df)
        
        if len(result_df) > 50:  # Need reasonable sample size
            category_counts = result_df['issue_category'].value_counts(normalize=True)
            
            # Shipping delays should be most common (expect ~25%)
            if 'shipping_delay' in category_counts:
                shipping_pct = category_counts['shipping_delay']
                assert shipping_pct > 0.15, f"Shipping delays too rare: {shipping_pct:.1%}"

class TestPriorityAssignment:
    """Test priority level assignment"""
    
    def test_priority_levels_valid(self, sample_users_df, sample_orders_df):
        """Test that priority levels are from valid set"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            expected_priorities = ['low', 'medium', 'high']
            actual_priorities = set(result_df['priority'])
            
            assert actual_priorities.issubset(set(expected_priorities)), \
                f"Invalid priorities: {actual_priorities - set(expected_priorities)}"
    
    def test_priority_distribution(self, sample_users_df, sample_orders_df):
        """Test that priority distribution is realistic"""
        # Generate larger sample
        large_users = pd.concat([sample_users_df] * 10, ignore_index=True)
        large_users['user_id'] = range(1, len(large_users) + 1)
        
        result_df = generate_support_tickets(large_users, sample_orders_df)
        
        if len(result_df) > 50:
            priority_counts = result_df['priority'].value_counts(normalize=True)
            
            # Low priority should be most common
            if 'low' in priority_counts:
                low_pct = priority_counts['low']
                assert low_pct > 0.3, f"Low priority too rare: {low_pct:.1%}"
            
            # High priority should be least common
            if 'high' in priority_counts:
                high_pct = priority_counts['high']
                assert high_pct < 0.3, f"High priority too common: {high_pct:.1%}"

class TestStatusAssignment:
    """Test status assignment logic"""
    
    def test_status_values_valid(self, sample_users_df, sample_orders_df):
        """Test that status values are from valid set"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            expected_statuses = ['resolved', 'closed', 'in_progress', 'open']
            actual_statuses = set(result_df['status'])
            
            assert actual_statuses.issubset(set(expected_statuses)), \
                f"Invalid statuses: {actual_statuses - set(expected_statuses)}"
    
    def test_status_distribution_realistic(self, sample_users_df, sample_orders_df):
        """Test that most tickets are resolved"""
        # Generate larger sample
        large_users = pd.concat([sample_users_df] * 10, ignore_index=True)
        large_users['user_id'] = range(1, len(large_users) + 1)
        
        result_df = generate_support_tickets(large_users, sample_orders_df)
        
        if len(result_df) > 50:
            status_counts = result_df['status'].value_counts(normalize=True)
            
            # Most tickets should be resolved
            if 'resolved' in status_counts:
                resolved_pct = status_counts['resolved']
                assert resolved_pct > 0.5, f"Too few resolved tickets: {resolved_pct:.1%}"

class TestResolutionTime:
    """Test resolution time calculation logic"""
    
    def test_resolution_time_calculation(self):
        """Test resolution time calculation function"""
        # Test different issue types and priorities
        test_cases = [
            ('shipping_delay', 'high', 'resolved'),
            ('product_defect', 'medium', 'resolved'),
            ('general_inquiry', 'low', 'resolved'),
            ('billing_question', 'high', 'open'),  # Should return None
        ]
        
        for issue_category, priority, status in test_cases:
            resolution_time = calculate_resolution_time(issue_category, priority, status)
            
            if status in ['open', 'in_progress']:
                assert resolution_time is None, f"Open/in-progress tickets shouldn't have resolution time"
            else:
                assert resolution_time is not None, f"Resolved tickets should have resolution time"
                assert resolution_time > 0, f"Resolution time should be positive: {resolution_time}"
                assert resolution_time < 200, f"Resolution time seems too high: {resolution_time} hours"
    
    def test_priority_affects_resolution_time(self):
        """Test that higher priority issues resolve faster"""
        issue_category = 'shipping_delay'
        status = 'resolved'
        
        # Get resolution times for different priorities
        high_priority_times = [
            calculate_resolution_time(issue_category, 'high', status) 
            for _ in range(20)
        ]
        low_priority_times = [
            calculate_resolution_time(issue_category, 'low', status) 
            for _ in range(20)
        ]
        
        # High priority should generally be faster (on average)
        avg_high = sum(t for t in high_priority_times if t is not None) / len(high_priority_times)
        avg_low = sum(t for t in low_priority_times if t is not None) / len(low_priority_times)
        
        assert avg_high < avg_low, f"High priority ({avg_high:.1f}h) should be faster than low priority ({avg_low:.1f}h)"

class TestResponseTime:
    """Test first response time calculation"""
    
    def test_response_time_calculation(self):
        """Test first response time calculation"""
        test_cases = [
            ('high', 'live_chat'),
            ('medium', 'email'),
            ('low', 'contact_form'),
            ('high', 'phone'),
        ]
        
        for priority, channel in test_cases:
            response_time = calculate_first_response_time(priority, channel)
            
            assert response_time >= 1, f"Response time should be at least 1 minute: {response_time}"
            assert response_time <= 1000, f"Response time seems too high: {response_time} minutes"
    
    def test_channel_affects_response_time(self):
        """Test that different channels have different response times"""
        priority = 'medium'
        
        phone_time = calculate_first_response_time(priority, 'phone')
        email_time = calculate_first_response_time(priority, 'email')
        
        # Phone should generally be faster than email
        assert phone_time <= email_time, f"Phone ({phone_time}min) should be faster than email ({email_time}min)"

class TestTicketDescriptions:
    """Test ticket description generation"""
    
    def test_description_generation(self):
        """Test that descriptions are generated for all categories"""
        categories = [
            'shipping_delay', 'product_defect', 'return_request',
            'billing_question', 'account_issue', 'general_inquiry'
        ]
        
        for category in categories:
            description = generate_ticket_description(category, pd.DataFrame())
            
            assert isinstance(description, str), f"Description should be string for {category}"
            assert len(description) > 10, f"Description too short for {category}: {description}"
            assert len(description) < 200, f"Description too long for {category}: {description}"

class TestSatisfactionScores:
    """Test satisfaction score assignment"""
    
    def test_satisfaction_scores_valid_range(self, sample_users_df, sample_orders_df):
        """Test that satisfaction scores are in valid range"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            scores = result_df['satisfaction_score']
            
            assert all(scores >= 1), f"All satisfaction scores should be >= 1"
            assert all(scores <= 5), f"All satisfaction scores should be <= 5"
            assert all(scores == scores.astype(int)), "Satisfaction scores should be integers"
    
    def test_satisfaction_correlates_with_status(self, sample_users_df, sample_orders_df):
        """Test that resolved tickets have higher satisfaction"""
        # Generate larger sample
        large_users = pd.concat([sample_users_df] * 20, ignore_index=True)
        large_users['user_id'] = range(1, len(large_users) + 1)
        
        result_df = generate_support_tickets(large_users, sample_orders_df)
        
        if len(result_df) > 100:
            # Group by status and check average satisfaction
            satisfaction_by_status = result_df.groupby('status')['satisfaction_score'].mean()
            
            if 'resolved' in satisfaction_by_status and 'open' in satisfaction_by_status:
                resolved_avg = satisfaction_by_status['resolved']
                open_avg = satisfaction_by_status['open']
                
                assert resolved_avg > open_avg, \
                    f"Resolved tickets ({resolved_avg:.1f}) should have higher satisfaction than open ({open_avg:.1f})"

class TestDataQuality:
    """Test data quality and validation"""
    
    def test_no_null_critical_fields(self, sample_users_df, sample_orders_df):
        """Test that critical fields are never null"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            critical_fields = [
                'ticket_id', 'user_id', 'issue_category', 'priority',
                'status', 'satisfaction_score', 'contact_channel'
            ]
            
            for field in critical_fields:
                null_count = result_df[field].isnull().sum()
                assert null_count == 0, f"Field {field} has {null_count} null values"
    
    def test_ticket_ids_unique(self, sample_users_df, sample_orders_df):
        """Test that ticket IDs are unique"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 1:
            ticket_ids = result_df['ticket_id'].tolist()
            unique_ids = set(ticket_ids)
            
            assert len(ticket_ids) == len(unique_ids), "All ticket IDs should be unique"
    
    def test_date_consistency(self, sample_users_df, sample_orders_df):
        """Test that ticket dates are after user registration"""
        result_df = generate_support_tickets(sample_users_df, sample_orders_df)
        
        if len(result_df) > 0:
            # Merge with user data to check dates
            merged = result_df.merge(sample_users_df[['user_id', 'registration_date']], on='user_id')
            
            for _, row in merged.iterrows():
                ticket_date = pd.to_datetime(row['created_date'])
                reg_date = pd.to_datetime(row['registration_date'])
                
                assert ticket_date >= reg_date, \
                    f"Ticket date {ticket_date} should be after registration {reg_date}"

class TestEdgeCases:
    """Test edge cases and error conditions"""
    
    def test_empty_users_handling(self):
        """Test handling of empty users dataframe"""
        empty_users = pd.DataFrame({
            'user_id': [],
            'email': [],
            'registration_date': []
        })
        empty_orders = pd.DataFrame({
            'order_id': [],
            'user_id': []
        })
        
        result_df = generate_support_tickets(empty_users, empty_orders)
        
        # Should return empty DataFrame without errors
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        assert len(result_df) == 0, "Should return empty DataFrame for empty input"
    
    def test_users_without_orders(self, sample_users_df):
        """Test handling of users with no orders"""
        empty_orders = pd.DataFrame({
            'order_id': [],
            'user_id': [],
            'order_date': []
        })
        
        result_df = generate_support_tickets(sample_users_df, empty_orders)
        
        # Should handle gracefully (users can still contact support)
        assert isinstance(result_df, pd.DataFrame), "Should return DataFrame"
        # May or may not generate tickets, but shouldn't crash

if __name__ == "__main__":
    # Run tests
    pytest.main([__file__, "-v"])