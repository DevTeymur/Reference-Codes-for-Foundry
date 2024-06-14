import pandas as pd

main = pd.read_csv('main.csv')
copy = pd.read_csv('copy.csv')


unique_well_names = list(main['Wellname'].unique())

def check_one_row(wellname, main, copy):
    main = main[main['Wellname'] == wellname].reset_index(drop=True)
    copy = copy[copy['Wellname'] == wellname].reset_index(drop=True)
    changes = []

    properties_to_check = list(main.columns)
    for property in properties_to_check:
        m = main.loc[0, property]
        c = copy.loc[0, property]
        if m != c and (not pd.isna(m) and not pd.isna(c)):
            print(f'{property} has changed from {c} to {m}')
            changes.append(f'{property} has changed from {c} to {m}')
    return changes

all_changes = []
for well in unique_well_names:
    chages_in_well = check_one_row(well, main, copy)
    all_changes = all_changes + chages_in_well

changes_df = pd.DataFrame(data = {'changes': all_changes})

print(changes_df)
changes_df.to_csv('changes.csv', index=False)

if True:
    main.to_csv('copy.csv', index=False)