import pandas as pd
data = pd.read_csv("adult.data.csv")
data.head()

#первая задача
sex_counts = data["sex"].value_counts()

#вторая задача
male_age_counts = data[data["sex"] == 'Male']['age'].mean()

#третья задача
us_counts = (data["native-country"] == 'United-States').mean()

#четвертая-пятая задача
rich = data[data["salary"] == '>50K']
poor = data[data["salary"] == '<=50K']
rich_avg = rich["age"].mean()
poor_avg = poor["age"].mean()
rich_std = rich['age'].std()
poor_std = poor['age'].std()

#шестая задача
high_ed = ['Bachelors', 'Prof-school', 'Assoc-acdm', 'Assoc-voc', 'Masters', 'Doctorate']
have_rich_ed = rich['education'].isin(high_ed)
is_true = have_rich_ed.all()

#седьмая задача
race_age_stat = data.groupby(['race', 'sex'])['age'].describe()
max_age_male = race_age_stat.loc[('Asian-Pac-Islander', 'Male')]['max']

#восьмая задача
married = rich['marital-status'].str.startswith('Married')
unmarried = ~married
rich_married = rich[married]
rich_unmarried = rich[unmarried]
dif_married = rich_married.shape[0] / rich.shape[0]
dif_unmarried = rich_unmarried.shape[0] / rich.shape[0]
if dif_married > dif_unmarried:
    print('Женатые зарабатывают больше')
else:
    print('Неженатые зарабатывают больше')

#девятая задача
working_hours = data['hours-per-week'].max()
employees_working_hours = data[data['hours-per-week'] == working_hours]
count_working_hours = employees_working_hours.shape[0]
rich_count = employees_working_hours[employees_working_hours['salary'] == '>50K'].shape[0]
rich_percentage = rich_count / count_working_hours * 100

#десятая задача
avg_working_hours = data.groupby(['salary', 'native-country'])['hours-per-week'].mean()

#одиннадцатая задача
data['age-group'] = ''
data.loc[(data['age'] >= 16) & (data['age'] < 35), 'age-group'] = 'young'
data.loc[(data['age'] >= 35) & (data['age'] < 70), 'age-group'] = 'adult'
data.loc[(data['age'] >= 70) & (data['age'] < 100), 'age-group'] = 'retiree'

#12-13 задача
rich = data[data['salary'] == '>50K']
rich_group_count = rich.groupby('age-group').size()
most_rich = rich_group_count.idxmax()
print(rich_group_count)
print(most_rich)

#14 задача
occupation_counts = data.groupby('occupation').size()
print(occupation_counts)
def filter_func(group):
    mean_age = group['age'].mean() <= 40
    min_hours = (group['hours-per-week'] > 5).all()
    return mean_age and min_hours