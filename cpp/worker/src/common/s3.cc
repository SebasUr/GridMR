#include "gridmr/worker/common/s3.h"
#include "gridmr/worker/common/env.h"
#include <fstream>
#include <iostream>
#include <sstream>
#include <cstdlib>

#ifdef GRIDMR_HAVE_AWSSDK
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectRequest.h>
#endif

namespace gridmr_worker {

bool parse_s3_uri(const std::string& uri, S3Loc& out){
  const std::string pre = "s3://";
  if (uri.rfind(pre, 0) != 0) return false;
  auto rest = uri.substr(pre.size());
  auto slash = rest.find('/');
  if (slash == std::string::npos) return false;
  out.bucket = rest.substr(0, slash);
  out.key = rest.substr(slash + 1);
  return !out.bucket.empty() && !out.key.empty();
}

#ifdef GRIDMR_HAVE_AWSSDK
static void ensureAwsInit(){
  static bool inited = false;
  static Aws::SDKOptions options;
  if (!inited){
    Aws::InitAPI(options);
    std::atexit([](){ static Aws::SDKOptions options2; Aws::ShutdownAPI(options2); });
    inited = true;
  }
}

static std::string strip_scheme(const std::string& endpoint, Aws::Http::Scheme& scheme){
  std::string ep = endpoint;
  scheme = Aws::Http::Scheme::HTTPS;
  const std::string http = "http://";
  const std::string https = "https://";
  if (ep.rfind(http,0)==0){ scheme = Aws::Http::Scheme::HTTP; return ep.substr(http.size()); }
  if (ep.rfind(https,0)==0){ scheme = Aws::Http::Scheme::HTTPS; return ep.substr(https.size()); }
  return ep; // host:port
}

static bool s3_download_to_file_sdk(const std::string& s3_uri, const std::string& out_path){
  ensureAwsInit();
  S3Loc loc; if (!parse_s3_uri(s3_uri, loc)){ std::cerr << "[worker] Bad S3 URI: " << s3_uri << std::endl; return false; }
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");

  Aws::Client::ClientConfiguration cfg;
  cfg.region = "us-east-1";
  cfg.useDualStack = false;
  cfg.enableHostPrefixInjection = false;
  cfg.followRedirects = true;
  cfg.verifySSL = false;
  cfg.useVirtualAddressing = false;
  Aws::Http::Scheme scheme;
  cfg.endpointOverride = strip_scheme(endpoint, scheme).c_str();
  cfg.scheme = scheme;

  Aws::Auth::AWSCredentials creds(access.c_str(), secret.c_str());
  Aws::S3::S3Client s3(creds, cfg, Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, /*useVirtualAddressing*/ false);

  Aws::S3::Model::GetObjectRequest req;
  req.SetBucket(loc.bucket.c_str());
  req.SetKey(loc.key.c_str());
  auto outcome = s3.GetObject(req);
  if (!outcome.IsSuccess()){
    std::cerr << "[worker] S3 GetObject failed: " << outcome.GetError().GetMessage() << std::endl;
    return false;
  }
  auto& is = outcome.GetResult().GetBody();
  std::ofstream ofs(out_path, std::ios::binary);
  ofs << is.rdbuf();
  ofs.close();
  return true;
}
#endif

static bool s3_download_to_file_cli(const std::string& s3_uri, const std::string& out_path){
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");
  std::string cmd = std::string("AWS_ACCESS_KEY_ID=") + access +
                    " AWS_SECRET_ACCESS_KEY=" + secret +
                    " aws --no-cli-pager --endpoint-url " + endpoint +
                    " s3 cp '" + s3_uri + "' '" + out_path + "'";
  std::cerr << "[worker] awscli: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}

bool download_url_to_file(const std::string& url, const std::string& out_path){
  if (url.rfind("s3://", 0) == 0) {
#ifdef GRIDMR_HAVE_AWSSDK
    if (s3_download_to_file_sdk(url, out_path)) return true;
#endif
    return s3_download_to_file_cli(url, out_path);
  }
  std::string cmd = std::string("curl -fsSL ") + url + " -o " + out_path;
  std::cerr << "[worker] curl: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}

bool upload_file_to_minio(const std::string& local_path, const std::string& s3_uri){
  std::string endpoint = envOr("MINIO_ENDPOINT", "http://minio:9000");
  std::string access = envOr("MINIO_ACCESS_KEY", "minioadmin");
  std::string secret = envOr("MINIO_SECRET_KEY", "minioadmin");
  std::string cmd = std::string("AWS_ACCESS_KEY_ID=") + access +
                    " AWS_SECRET_ACCESS_KEY=" + secret +
                    " aws --no-cli-pager --endpoint-url " + endpoint +
                    " s3 cp '" + local_path + "' '" + s3_uri + "'";
  std::cerr << "[worker] awscli upload: " << cmd << std::endl;
  int rc = std::system(cmd.c_str());
  return rc == 0;
}

} // namespace gridmr_worker
