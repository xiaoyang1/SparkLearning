package learn.cf;

import java.util.*;

public class ItemCFDemo {
    //系统用户
    private static String[] users={"小明","小花","小美","小张","小李"};
    //和这些用户相关的电影
    private static String[] movies={"电影1","电影2","电影3","电影4","电影5","电影6","电影7"};

    // 用户点评电影情况
    private static Integer[][] allUserMovieCommentList = {
            {1,1,1,0,1,0,0},
            {0,1,1,0,0,1,0},
            {1,0,1,1,1,1,1},
            {1,1,1,1,1,0,0},
            {1,1,0,1,0,1,1}
    };
    // 用户点评电影情况， 行转列
    private static Integer[][] allMovieCommentList = new Integer[allUserMovieCommentList[0].length][allUserMovieCommentList.length];
    // 电影相似度
    private static HashMap<String, Double> movieABSimilaritys = null;

    // 待推荐电影的相似度列表
    private static HashMap<Integer, Object> movieSimilaritys = null;
    // 用户的index
    private static Integer targetUserIndex = null;
    // 目标用户点评过的电影
    private static List<Integer> targetUserCommentedMovies = null;
    // 推荐电影
    private static List<Map.Entry<Integer, Object>> recommlist = null;

    public static void main(String[] args) {
        Scanner in = new Scanner(System.in);
        String user = in.nextLine();

        while (user != null && !"exit".equals(user)) {
            targetUserIndex = findUserIndex(user);
            if (targetUserIndex == null) {
                System.out.println("没有搜索到此用户，请重新输入：");
            } else {
                // 转换目标用户电影点评列表
                targetUserCommentedMovies = Arrays.asList(allUserMovieCommentList[targetUserIndex]);
                // 计算电影相似度
                calcAllMovieSimilaritys();
                //获取全部待推荐电影
                calcRecommendMovie();
                //输出推荐电影
                System.out.print("推荐电影列表：");
                for (Map.Entry<Integer, Object> item:recommlist){
                    System.out.print(movies[item.getKey()]+"  ");
                }
                System.out.println();

            }
            user = in.nextLine();
        }
    }

    // 获得全部待推荐的电影
    private static void calcRecommendMovie() {
        movieSimilaritys = new HashMap<>();
        for(int i = 0; i < targetUserCommentedMovies.size() - 1; i++){
            for (int j = i + 1; j < targetUserCommentedMovies.size(); j++){
                Object similarity = null;
                if(targetUserCommentedMovies.get(i) == 1 && targetUserCommentedMovies.get(j) == 0
                        && movieABSimilaritys.get(i + " " + j) != null){
                    similarity = movieABSimilaritys.get(i + " " + j);
                    movieSimilaritys.put(j, similarity);
                } else if(targetUserCommentedMovies.get(i) == 0 && targetUserCommentedMovies.get(j) == 1
                        && movieABSimilaritys.get(i + " " + j) != null){
                    similarity = movieABSimilaritys.get(i + " " + j);
                    movieSimilaritys.put(i, similarity);
                }
            }
        }
        recommlist = new ArrayList<Map.Entry<Integer, Object>>(movieSimilaritys.entrySet());
        Collections.sort(recommlist, Comparator.comparing(a -> (Double)a.getValue()));
        System.out.println("待推荐相似度电影列表："+recommlist);
    }

    /**
     *  计算所有物品间的相似度
     */
    private static void calcAllMovieSimilaritys() {
        // 相当于基于用户的 矩阵行转列后， 使用基于用户的CF
        convertRow2Col();
        // 转换完毕后， 电影就是之前的用户，现在计算用户之间的相似度
        movieABSimilaritys = new HashMap<>();
        for(int i = 0; i< allMovieCommentList.length-1; i++){
            for(int j = i+1; j < allMovieCommentList.length; j++){
                movieABSimilaritys.put(i + " " + j, calcTwoMovieCommentSimilarity(allMovieCommentList[i], allMovieCommentList[j]));
            }
        }
        System.out.println("电影相似度："+movieABSimilaritys);
    }

    private static Double calcTwoMovieCommentSimilarity(Integer[] movie1Star, Integer[] movie2Star) {
        // 欧几里得距离
        float sum = 0;
        for(int i = 0; i < movie1Star.length; i++){
            sum += Math.pow(movie1Star[i] - movie2Star[i], 2);
        }
        return Math.sqrt(sum);

        // Jaccard 距离, 其实在0,1 的时候几乎等价于欧几里得的效果
    }

    private static void convertRow2Col() {
        for(int i = 0; i < allUserMovieCommentList[0].length; i++){
            for(int j = 0; j < allUserMovieCommentList.length; j++){
                allMovieCommentList[i][j] = allUserMovieCommentList[j][i];
            }
        }
        System.out.println("电影点评转行列："+Arrays.deepToString(allMovieCommentList));
    }

    private static Integer findUserIndex(String user) {
        if(user == null || "".equals(user)){
            return null;
        }
        for (int i = 0; i < users.length; i++){
            if(user.equals(users[i])){
                return i;
            }
        }
        return null;
    }
}
