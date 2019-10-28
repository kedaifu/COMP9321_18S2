import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def question_1(file_1='Olympics_dataset1.csv', file_2='Olympics_dataset2.csv'):
    df_winter = pd.read_csv(file_2, skiprows=1)
    df_summer = pd.read_csv(file_1, skiprows=1)
    df_winter.rename(columns={'Unnamed: 0':'Country'}, inplace=True)
    df_summer.rename(columns={'Unnamed: 0':'Country'}, inplace=True)
    result1 = pd.merge(df_summer, df_winter, how='left', left_on='Country', right_on='Country')
    result1 = result1.rename(index=str, columns={"Number of Games the country participated in_x": "Number of Games the country participated in summer"})
    result1 = result1.rename(index=str, columns={"Gold_x":"Gold_summer","Silver_x":"Silver_summer","Bronze_x":"Bronze_summer"})
    result1 = result1.rename(index=str, columns={"Number of Games the country participated in_y": "Number of Games the country participated in winter"})
    result1 = result1.rename(index=str, columns={"Gold_y":"Gold_winter","Silver_y":"Silver_winter","Bronze_y":"Bronze_winter"})
    result1 = result1.rename(index=str, columns={"Total_x":"Total_summer", "Total_y":"Total_winter","Total.1":"Total"})
    result1 = result1.rename(index=str, columns={"Gold.1":"Gold_total", "Silver.1":"Silver_total","Bronze.1":"Bronze_total"})
    result1 = result1.rename(index=str, columns={"Number of Games the country participated in.1": "Number of Games the country participated in total"})
    return result1

def question_2():
    result2 = question_1()
    result2 = result2.set_index('Country')
     
    result2 = result2.head(1)
    return result2

def question_3():
    result3 = question_1()
    result3 = result3.drop('Rubish', axis=1)
    result3 = result3.head(5)
    return result3

def question_4():
    result4 = question_1()
    result4 = result4.dropna()
    result4 = result4.tail(10)
    return result4

def question_5():
    result5 = question_1()
    result5 = result5.dropna(subset=['Gold_summer'])
    result5['Gold_summer'] = result5['Gold_summer'].apply(lambda x: x.replace(',',''))
    result5['Gold_summer'] = result5['Gold_summer'].astype('float')
    result5 = result5.sort_values(by=['Gold_summer'])
    
    result5 = result5.iloc[[-2]]
    return result5

def question_6():
    
    result6 = question_1()
    result6 = result6.dropna(subset=['Gold_summer'])
     
    result6['Gold_summer'] = result6['Gold_summer'].apply(lambda x: x.replace(',',''))
    result6['Gold_summer'] = result6['Gold_summer'].astype('float')
    
    result6['Gold_winter'] = result6['Gold_winter'].apply(lambda x: x.replace(',',''))
    result6['Gold_winter'] = result6['Gold_winter'].astype('float')

    newSeries = abs(result6['Gold_summer'] - result6['Gold_winter'])
    newSeries
    result6['Diff'] = pd.Series(newSeries, index=result6.index)
    result6 = result6.sort_values(by=['Diff'])
    result6 = result6.iloc[[-2]]
    return result6

def question_7(): 
    result7 = question_1()
    result7['Total'] = result7['Total'].astype(str)
    result7['Total'] = result7['Total'].apply(lambda x: x.replace(',',''))
    result7['Total'] = result7['Total'].astype('float')
    result7 = result7.sort_values(by=['Total'],ascending=False)
    result7 = pd.concat([result7.head(5), result7.tail(5)]) 
    return result7


def question_8(): 
    
    result8 = question_1()
    
    result8 = result8.dropna()
    
    result8['Total_summer'] = result8['Total_summer'].apply(lambda x: x.replace(',',''))
    result8['Total_summer'] = result8['Total_summer'].astype('float')
    
    result8['Total_winter'] = result8['Total_winter'].apply(lambda x: x.replace(',',''))
    result8['Total_winter'] = result8['Total_winter'].astype('float')
    
    result8['Total'] = result8['Total'].apply(lambda x: x.replace(',',''))
    result8['Total'] = result8['Total'].astype('float')
    
    result8 = result8.sort_values(by='Total', ascending=False)
    
    summer = tuple(result8.iloc[1:11,6].tolist())
    winter = tuple(result8.iloc[1:11,11].tolist())
    country = tuple(result8.iloc[1:11,0].tolist())
    
    N = 10
 
    ind = np.arange(N)     
    width = 0.5       
    p1 = plt.barh(ind, summer, width)
    p2 = plt.barh(ind, winter, width,
                 left=summer)
    plt.gca().invert_yaxis()
    plt.title('total medals for winter and summer games')
    plt.yticks(ind, country)
    plt.yticks(np.arange(0, 10, 1))
    plt.legend((p1[0], p2[0]), ('summer', 'winter'))

    plt.show()

    
def question_9(): 
    
    result9 = question_1()
    result9 = result9[result9['Country'].str.contains('United States|Australia|Great Britain|Japan|New Zealand')]
    
    result9['Gold_winter'] = result9['Gold_winter'].apply(lambda x: x.replace(',',''))
    result9['Gold_winter'] = result9['Gold_winter'].astype('float')
    
    result9['Silver_winter'] = result9['Silver_winter'].apply(lambda x: x.replace(',',''))
    result9['Silver_winter'] = result9['Silver_winter'].astype('float')
    
    result9['Bronze_winter'] = result9['Bronze_winter'].apply(lambda x: x.replace(',',''))
    result9['Bronze_winter'] = result9['Bronze_winter'].astype('float')
    
    N = 5
 
    ind = np.arange(N) 
    width = 0.5
    
    p1 = plt.bar(ind*2-width, result9['Gold_winter'], width)
    p2 = plt.bar(ind*2 , result9['Silver_winter'], width)
    p3 = plt.bar(ind*2 + width, result9['Bronze_winter'], width)
    plt.xticks(ind*2, result9['Country'],rotation=90)
    
    plt.yticks(np.arange(0, 100, 10))
    plt.legend((p1[0], p2[0], p3[0]), ('Gold', 'Silver','Bronze'))
    
    plt.show()
    


if __name__ == "__main__":
    
    print("Question 1: ")
    q1 = question_1().head(5).to_string()
    print(q1)
    print("--------------------------------------------------------------------------------\n")

    print("Question 2: ")
    q2 = question_2()
    print(q2.to_string())
    print("--------------------------------------------------------------------------------\n")
   

    print("Question 3: ")
    q3 = question_3()
    print(q3.to_string())
    print("--------------------------------------------------------------------------------\n")

    print("Question 4: ")
    q4 = question_4()
    print(q4.to_string())
    print("--------------------------------------------------------------------------------\n")


    print("Question 5: ")
    q5 = question_5()
    print(q5.to_string())
    print("--------------------------------------------------------------------------------\n")



    print("Question 6: ")
    q6 = question_6()
    print(q6.to_string())
    print("--------------------------------------------------------------------------------\n")


    print("Question 7: ")
    q7 = question_7()
    print(q7.to_string())
    print("--------------------------------------------------------------------------------\n")

    question_8()


    question_9()
    


