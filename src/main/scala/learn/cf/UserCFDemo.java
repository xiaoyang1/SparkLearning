package learn.cf;

import java.util.*;

public class UserCFDemo {

    //系统用户
    private static String[] users={"小明","小花","小美","小张","小李"};
    //和这些用户相关的电影
    private static String[] movies={"电影1","电影2","电影3","电影4","电影5","电影6","电影7"};
    // 用户点评电影打分数据， 是users 对应用户针对movies 对应电影的评分
    private static int[][] allUserMovieStarList = {
            {3,1,4,4,1,0,0},
            {0,5,1,0,0,4,0},
            {1,0,5,4,3,5,2},
            {3,1,4,3,5,0,0},
            {5,2,0,1,0,5,5}
    };
    // 相似用户的集合
    private static List<List<Object>> similarityUsers = null;
    // 推荐所有电影集合
    private static List<String> targetRecommendMovies = null;
    // 点评过的电影集合
    private static List<String> commentedMovies = null;
    // 用户在电影打星集合中的位置。
    private static Integer targetUserIndex = null;


    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String user = in.nextLine();

        while (user != null && !"exit".equals(user)){
            targetUserIndex = findUserIndex(user);
            if(targetUserIndex==null){
                System.out.println("没有搜索到此用户，请重新输入：");
            } else {
                // 计算用户的相似度
                calcUserSimilarity();
                // 计算电影推荐度， 排序
                calcRecommendMovie();
                // 处理推荐电影列表
                handleRecommendMovies();

                //输出推荐电影
                System.out.print("推荐电影列表：");
                for (String item:targetRecommendMovies){
                    if(!commentedMovies.contains(item)){
                        System.out.print(item+"  ");
                    }
                }
                System.out.println();
            }
            user = in.nextLine();
            targetRecommendMovies = null;
        }
    }

    private static void handleRecommendMovies() {
        commentedMovies = new ArrayList<>();
        for(int i = 0; i < allUserMovieStarList[targetUserIndex].length; i++){
            if(allUserMovieStarList[targetUserIndex][i] != 0){
                commentedMovies.add(movies[i]);
            }
        }
    }

    private static void calcRecommendMovie() {
        targetRecommendMovies = new ArrayList<>();
        List<List<Object>> recommendMovies = new ArrayList<>();
        List<Object> recommendMovie = null;

        double recommdRate = 0, sumRate = 0;
        for(int i = 0; i < movies.length; i++){
            recommendMovie = new ArrayList<>();
            recommendMovie.add(i);
            recommdRate = allUserMovieStarList[Integer.parseInt(similarityUsers.get(0).get(0).toString())][i] + Double.parseDouble(similarityUsers.get(0).get(1).toString())
                    + allUserMovieStarList[Integer.parseInt(similarityUsers.get(1).get(0).toString())][i] + Double.parseDouble(similarityUsers.get(1).get(1).toString());

            recommendMovie.add(recommdRate);
            recommendMovies.add(recommendMovie);
            sumRate += recommdRate;
        }
        // 排序
        Collections.sort(recommendMovies, Comparator.comparing(a -> Double.valueOf(a.get(1).toString())));
        final double tempSumRate = sumRate;
        recommendMovies.stream().filter(a -> Double.valueOf(a.get(1).toString()) > tempSumRate/7)
                .forEach(a -> targetRecommendMovies.add(movies[Integer.parseInt(a.get(0).toString())]));
        
    }

    // 计算用户间的相似度，并排序，取出相似度最高的用户
    private static void calcUserSimilarity() {
        similarityUsers = new ArrayList<>();

        // 计算与所有人之间的相似度
        List<List<Object>> userSimilaritys=new ArrayList<>();
        for(int i = 0; i<users.length; i++){
            if(i == targetUserIndex){
                continue;
            }
            List<Object> doubleSimilarity = new ArrayList<>();
            // 把自己当前的索引加上
            doubleSimilarity.add(i);
            doubleSimilarity.add(calcTwoUserSimilarity(allUserMovieStarList[i], allUserMovieStarList[targetUserIndex]));
            userSimilaritys.add(doubleSimilarity);
        }
        // 排序
        Collections.sort(userSimilaritys, Comparator.comparing(a -> Double.valueOf(a.get(1).toString())));

        // 打印一波
        for(List<Object> each : userSimilaritys){
            System.out.println(each.get(0) + "   :   " + each.get(1));
        }
        // 取出距离最小最相似的前两个
        similarityUsers.add(userSimilaritys.get(0));
        similarityUsers.add(userSimilaritys.get(1));
    }

    // 计算预先相似度
    private static Object calcTwoUserSimilarity(int[] user1Stars, int[] user2Stars) {
        float sum = 0;
        for(int i = 0; i < user1Stars.length; i++){
            sum += Math.pow(user1Stars[i] - user2Stars[i], 2);
        }
        return Math.sqrt(sum);
    }

    private static Integer findUserIndex(String user) {
        if(user == null || "".equals(user)){
            return null;
        }
        for(int i = 0; i<users.length; i++){
            if(user.equals(users[i])){
                return i;
            }
        }
        return null;
    }
}
