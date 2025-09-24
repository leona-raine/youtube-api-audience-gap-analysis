from sklearn.preprocessing import StandardScaler

features = df[["views", "likes", "comments", "engagement"]]
X = StandardScaler().fit_transform(features)