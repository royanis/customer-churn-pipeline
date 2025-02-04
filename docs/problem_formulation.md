#### Problem Formulation ####

## Business Problem
Customer churn significantly impacts revenue, as losing existing customers forces companies to spend more on acquiring new ones. This churn can lead to direct revenue loss and indirect effects—such as reduced customer loyalty and increased competition—making it imperative to understand and predict which customers are at risk of leaving.

## Business Objectives
- **Reduce Churn Rate:** Identify at-risk customers early so that targeted retention strategies can be implemented.
- **Improve Customer Retention:** Enhance customer loyalty by proactively addressing churn triggers.
- **Optimize Resource Allocation:** Lower the overall costs by focusing on retention rather than expensive customer acquisition.

## Key Data Sources and Their Attributes
1. **CSV Files:**
   - **Customer Demographics:** Attributes include age, gender, income, location, etc.
   - **Transaction History:** Details such as transaction date, amount, product purchased.
2. **REST APIs:**
   - **Web Logs:** Session duration, pages visited, clickstream data.
   - **Customer Interactions:** Data from customer support tickets, feedback, and service usage.

## Expected Pipeline Outputs
- **Clean Datasets for EDA:** Data that has been validated, cleaned, and is ready for exploratory data analysis.
- **Transformed Features for Machine Learning:** Engineered features (e.g., customer tenure, total spend per customer) that are preprocessed and normalized for model training.
- **Deployable Predictive Model:** A trained model that forecasts customer churn, stored in a versioned format (e.g., `.pkl` for scikit-learn models or `.h5` for TensorFlow/Keras models).

## Evaluation Metrics
- **Accuracy:** The proportion of correctly predicted instances.
- **Precision:** The ratio of correctly predicted positive observations to the total predicted positives.
- **Recall:** The ratio of correctly predicted positive observations to all observations in the actual class.
- **F1 Score:** The weighted average of Precision and Recall, which balances the two metrics.