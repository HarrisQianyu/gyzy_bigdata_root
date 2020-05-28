package com.cn.tz14.springcloud.es;


import io.swagger.annotations.Api;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 * @author: KING
 * @description:
 * @Date:Created in 2019-10-21 14:16
 */
@Configuration
@EnableSwagger2  //启动Swagger2
@Api(value = "ES查询接口", tags = "ES查询接口")
public class SwaggerConfiguration {

    //api接口包扫描路径
    public static final String SWAGGER_SCAN_BASE_PACKAGE = "com.cn.tz14.springcloud.es.controller";

    public static final String VERSION = "1.0.0";
    @Bean
    public Docket createRestApi() {
        return new Docket(DocumentationType.SWAGGER_2)
                .apiInfo(apiInfo())
                .select()
                .apis(RequestHandlerSelectors.basePackage(SWAGGER_SCAN_BASE_PACKAGE))
                .paths(PathSelectors.any())
                .build();
    }
    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("ES查询API接口")  //设置文档的标题
                .description("swagger-bootstrap-ui") // 设置文档的描述
                .termsOfServiceUrl("wfewf")
               // .contact("developer@mail.com")
                .version(VERSION)  // 设置文档的版本信息-> 1.0.0 Version information
                .build();
    }
}
